#!/usr/bin/env python
"""Перевірка завершення класифікації та повідомлення на пошту.

Скрипт ставиться в чергу `at` на конкретний час. Кожен запуск:

- рахує залиш verified-документів, яким ще не призначено дисципліну;
- якщо залиш нульовий — надсилає лист «завершено» і НЕ планує наступну
  перевірку (робота закінчилась, таймер зупиняється);
- якщо залиш не зменшився від попередньої перевірки — лист «застрягло»
  (разом зі станом служби);
- якщо все нормально — лист не надсилається, а наступна перевірка
  планується через задану кількість годин.

Скрипт ніколи не змінює дані: він лише читає залиш і пише свій стан у
JSON-файл.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from harvester.config import get_settings
from harvester.core.notify import send_notification
from harvester.db.failover import build_database

DEFAULT_STATE = "/opt/harvester/data/classification_check_state.json"
SCRIPT_PATH = str(Path(__file__).resolve())


async def count_backlog() -> int:
    """Скільки verified-документів ще без призначеної дисципліни."""
    settings = get_settings()
    db = build_database(settings)
    await db.initialize(sync_mirror=False, read_only=True)
    try:
        row = await db.fetchone(
            """
            SELECT COUNT(*) AS n FROM documents
            WHERE status = 'verified' AND discipline_checked_at IS NULL
            """
        )
        return int(row["n"] or 0) if row else 0
    finally:
        await db.close()


def load_state(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {}


def save_state(path: str, state: dict) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state, fh, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def service_state() -> str:
    """Стан служби harvester — щоб лист показував, чи сервіс живий."""
    try:
        out = subprocess.run(
            ["systemctl", "is-active", "harvester"],
            capture_output=True, text=True, timeout=10, check=False,
        )
        return out.stdout.strip() or "unknown"
    except Exception:  # noqa: BLE001
        return "unknown"


def schedule_next(interval_h: float, state_path: str) -> str:
    """Поставити наступну перевірку через interval_h годин через `at`."""
    cmd = (
        f"cd /opt/harvester && /opt/harvester/venv/bin/python {SCRIPT_PATH} "
        f"--interval-h {interval_h:g} --state {state_path}"
    )
    proc = subprocess.run(
        ["at", "now", "+", f"{int(interval_h)} hours"],
        input=cmd, text=True, capture_output=True, timeout=30, check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"at не прийняв задачу: {proc.stderr.strip()}")
    return proc.stdout.strip()


def build_body(backlog: int, state: dict, svc: str) -> str:
    now = datetime.now(UTC).strftime("%d.%m.%Y %H:%M UTC")
    last = state.get("last_backlog")
    done = state.get("last_done_total")
    lines = [
        f"Час перевірки: {now}",
        f"Стан служби harvester: {svc}",
        f"Залиш документів без дисципліни: {backlog}",
    ]
    if last is not None:
        lines.append(f"Попередня перевірка: залиш {last}")
    if done:
        lines.append(f"Оброблено загалом: {done}")
    return "\n".join(lines)


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval-h", type=float, default=12.0,
                        help="Через скільки годин планувати наступну перевірку")
    parser.add_argument("--state", default=DEFAULT_STATE, help="Файл стану (JSON)")
    parser.add_argument("--start-at", default="",
                        help="Не робити нічого раніше цього моменту (ISO)")
    parser.add_argument("--no-schedule", action="store_true",
                        help="Не планувати наступну перевірку")
    args = parser.parse_args()

    if args.start_at:
        start = datetime.fromisoformat(args.start_at)
        if start.tzinfo is None:
            start = start.replace(tzinfo=UTC)
        if datetime.now(UTC) < start:
            print(f"ще не час перевірки (до {args.start_at}), виходимо")
            return 0

    state = load_state(args.state)
    svc = service_state()

    try:
        backlog = await count_backlog()
    except Exception as e:  # noqa: BLE001
        # Недоступна БД — це подія, про яку треба повідомити: інакше
        # перевірка мовчки не відбудеться і ніхто не дізнається.
        await send_notification(
            "Harvester: перевірка класифікації — помилка доступу до БД",
            f"Час: {datetime.now(UTC):%d.%m.%Y %H:%M UTC}\n"
            f"Стан служби: {svc}\nПомилка: {e}",
            key="classification_check_db_error",
            interval=timedelta(hours=1),
        )
        if not args.no_schedule:
            schedule_next(args.interval_h, args.state)
        return 1

    prev = state.get("last_backlog")
    stalled = prev is not None and backlog >= int(prev)
    finished = backlog == 0

    state.update({
        "last_checked_at": datetime.now(UTC).isoformat(),
        "last_backlog": backlog,
        "last_service_state": svc,
    })
    save_state(args.state, state)

    if finished:
        body = build_body(backlog, state, svc)
        body += (
            "\n\nКласифікація завершена: усі verified-документи мають "
            "призначену дисципліну.\nНаступні перевірки не заплановані."
        )
        await send_notification(
            "Harvester: класифікація завершена",
            body,
            key="classification_check_done",
            interval=timedelta(hours=1),
        )
        print(f"залиш {backlog} — завершено, лист надіслано")
        return 0

    if stalled:
        body = build_body(backlog, state, svc)
        body += (
            "\n\nУВАГА: залиш не зменшився від попередньої перевірки — "
            "робота, схоже, зупинилася або застрягла."
        )
        await send_notification(
            "Harvester: класифікація не просувається",
            body,
            key="classification_check_stalled",
            interval=timedelta(hours=1),
        )
        print(f"залиш {backlog} — без прогресу, лист надіслано")
    else:
        print(f"залиш {backlog} (було {prev}) — все гаразд, лист не надсилається")

    if not args.no_schedule:
        try:
            out = schedule_next(args.interval_h, args.state)
            print(f"наступну перевірку заплановано: {out}")
        except Exception as e:  # noqa: BLE001
            print(f"НЕ вдалося запланувати наступну перевірку: {e}", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
