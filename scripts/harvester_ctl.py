#!/usr/bin/env python3
"""Керування сервісом Harvester для агентів: up / get / status / down.

Один і той самий цикл у всіх випадках:

    python scripts/harvester_ctl.py up
    python scripts/harvester_ctl.py get --topic "Економіка" --count 10 --dest <ваш_проєкт>
    # ...працюєте з файлами, потім видаляєте скопійовану папку...
    python scripts/harvester_ctl.py down

Що робить кожна команда:

  up      — піднімає контейнер PostgreSQL (harvester-pg) і сервіс `harvester
            start`, чекає heartbeat (сервіс справді готовий). Ідемпотентна:
            повторний виклик не запускає другу копію. PID пише у
            data/agent_service.pid.
  get     — синхронно готує каталог джерел: LLM-відбір `curator prepare`
            (тема + кількість) → перевірка URL → завантаження PDF. Без --dest
            файли лягають у catalogs/catalog_*; з --dest — одразу у ваш проєкт.
            Повертає шлях до папки і підсумок. Типово 2–5 хв.
  status  — стан контейнера, сервісу (heartbeat) і останнього каталогу.
  down    — зупиняє сервіс за PID (з pidfile або по пошуку процесу).
            --deep додатково зупиняє контейнер PostgreSQL.

Правила безпеки: скрипт не чіпає БД і volume harvester-pgdata, не запускає
збір (режим обслуговування) і не видаляє каталоги сам — прибирання за собою
(видалення скопійованої папки) робить агент.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_IS_WIN = os.name == "nt"
_VENV = ROOT / ".venv" / ("Scripts" if _IS_WIN else "bin")
HARVESTER = _VENV / ("harvester.exe" if _IS_WIN else "harvester")
PYTHON = _VENV / ("python.exe" if _IS_WIN else "python")
CONFIG = ROOT / "config.yaml"
PIDFILE = ROOT / "data" / "agent_service.pid"
SERVICE_LOG = ROOT / "logs" / "agent_service.log"
CATALOGS = ROOT / "catalogs"
PG_CONTAINER = "harvester-pg"
PG_PORT = 5432

# Скільки чекати готовності/виконання (секунди).
READY_TIMEOUT_S = 180
PREPARE_TIMEOUT_S = 900
FETCH_TIMEOUT_S = 900
HEARTBEAT_MAX_AGE_S = 180


def _log(msg: str) -> None:
    print(msg, flush=True)


def _die(msg: str, code: int = 1) -> None:
    print(msg, file=sys.stderr, flush=True)
    raise SystemExit(code)


def _env() -> dict[str, str]:
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    return env


def _run(args: list[str], timeout: int | None = None, cwd: Path = ROOT) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(a) for a in args],
        cwd=str(cwd),
        env=_env(),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
    )


def _wait_port(port: int, timeout_s: int = 90) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=2):
                return True
        except OSError:
            time.sleep(1)
    return False


# ---------------------------------------------------------------- контейнер PG


def _docker(args: list[str], timeout: int = 120) -> subprocess.CompletedProcess:
    return _run(["docker", *args], timeout=timeout)


def _pg_ensure() -> None:
    """Контейнер PG має бути створений і запущений (ідемпотентно)."""
    if shutil.which("docker") is None:
        _die("Docker не знайдено в PATH — встановіть Docker Desktop і повторіть.")

    ps = _docker(["ps", "-a", "--filter", f"name=^{PG_CONTAINER}$", "--format", "{{.Names}} {{.Status}}"])
    line = (ps.stdout or "").strip()
    if not line:
        _log(f"Контейнер {PG_CONTAINER} не знайдено — створюю...")
        run = _docker([
            "run", "-d", "--name", PG_CONTAINER, "--restart", "unless-stopped",
            "-e", "POSTGRES_USER=harvester", "-e", "POSTGRES_PASSWORD=app5552",
            "-e", "POSTGRES_DB=harvester", "-p", f"127.0.0.1:{PG_PORT}:{PG_PORT}",
            "-v", "harvester-pgdata:/var/lib/postgresql/data",
            "postgres:17",
        ])
        if run.returncode != 0:
            _die(f"Не вдалося створити контейнер: {run.stderr or run.stdout}")
    elif "Up" not in line:
        _log(f"Запускаю контейнер {PG_CONTAINER}...")
        start = _docker(["start", PG_CONTAINER])
        if start.returncode != 0:
            _die(f"Не вдалося запустити контейнер: {start.stderr or start.stdout}")

    if not _wait_port(PG_PORT):
        _die(f"PostgreSQL не відповідає на 127.0.0.1:{PG_PORT} за 90 с. "
             f"Перевірте: docker logs {PG_CONTAINER}")


# ------------------------------------------------------------------ сервіс


def _heartbeat_age_s() -> int | None:
    """Вік heartbeat у секундах або None, якщо сервіс не відповідає."""
    if not HARVESTER.exists():
        return None
    try:
        out = _run([str(HARVESTER), "status"], timeout=120)
    except subprocess.TimeoutExpired:
        return None
    m = re.search(r"\((\d+)\s+с тому\)", out.stdout or "")
    if not m:
        return None
    return int(m.group(1))


def _pid_alive(pid: int) -> bool:
    if _IS_WIN:
        out = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            check=False,
        )
        return str(pid) in (out.stdout or "")
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _read_pidfile() -> int | None:
    try:
        return int(PIDFILE.read_text().strip())
    except (OSError, ValueError):
        return None


def _service_pids() -> list[int]:
    """PID процесів сервісу: спершу pidfile, потім пошук по командному рядку."""
    pid = _read_pidfile()
    if pid and _pid_alive(pid):
        return [pid]

    if _IS_WIN:
        # -like замість регексу: у PowerShell-рядку, переданому з Python,
        # backslash-епси ламалися, і жоден процес не знаходився.
        ps = (
            "Get-CimInstance Win32_Process | "
            "Where-Object { $_.Name -match 'harvester|python' -and "
            "$_.CommandLine -like '*harvester* start*' } | "
            "ForEach-Object { $_.ProcessId }"
        )
        out = _run(["powershell", "-NoProfile", "-Command", ps], timeout=60)
        pids = []
        for token in (out.stdout or "").split():
            if token.strip().isdigit():
                pids.append(int(token))
        # Наш власний процес (harvester_ctl.py) у список не потрапляє:
        # у його командному рядку немає "harvester start".
        return [p for p in pids if p != os.getpid()]

    out = _run(["pgrep", "-f", r"harvester.*start"], timeout=30)
    return [int(t) for t in (out.stdout or "").split() if t.strip().isdigit()]


def cmd_up(_args: argparse.Namespace) -> int:
    _pg_ensure()
    _log("✓ PostgreSQL: 127.0.0.1:5432 готовий")

    age = _heartbeat_age_s()
    pids = _service_pids()
    if age is not None and age <= HEARTBEAT_MAX_AGE_S and pids:
        PIDFILE.parent.mkdir(parents=True, exist_ok=True)
        PIDFILE.write_text(str(pids[0]))
        _log(f"✓ Сервіс уже працює (PID {pids[0]}, heartbeat {age} с тому)")
        return 0
    if pids:
        _log(f"Знайдено старий процес сервісу (PID {pids[0]}) без heartbeat — перезапускаю...")
        _stop_pids(pids)

    if not HARVESTER.exists():
        _die(f"Не знайдено {HARVESTER}. Створіть venv: uv venv --python 3.12 .venv "
             f'&& uv pip install --python .venv -e ".[dev]"')

    SERVICE_LOG.parent.mkdir(parents=True, exist_ok=True)
    flags = 0
    if _IS_WIN:
        flags = getattr(subprocess, "DETACHED_PROCESS", 0x00000008) | subprocess.CREATE_NEW_PROCESS_GROUP
    # Popen дублює handle для дитини, тому його можна закрити одразу після
    # запуску — сервіс переживе завершення нашого процесу.
    with open(SERVICE_LOG, "ab", buffering=0) as log_fh:
        proc = subprocess.Popen(
            [str(HARVESTER), "start", "--config", str(CONFIG)],
            cwd=str(ROOT),
            env=_env(),
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            creationflags=flags,
        )
    PIDFILE.parent.mkdir(parents=True, exist_ok=True)
    PIDFILE.write_text(str(proc.pid))
    _log(f"Сервіс запущено (PID {proc.pid}), чекаю heartbeat...")

    deadline = time.time() + READY_TIMEOUT_S
    while time.time() < deadline:
        if proc.poll() is not None:
            tail = _tail(SERVICE_LOG, 20)
            _die(f"Сервіс завершився з кодом {proc.returncode}.\n--- logs/agent_service.log ---\n{tail}")
        age = _heartbeat_age_s()
        if age is not None and age <= HEARTBEAT_MAX_AGE_S:
            _log(f"✓ Сервіс готовий (heartbeat {age} с тому)")
            return 0
        time.sleep(3)
    _die(f"Сервіс не подав heartbeat за {READY_TIMEOUT_S} с. Див. {SERVICE_LOG}")


def _tail(path: Path, lines: int = 20) -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            return "".join(fh.readlines()[-lines:])
    except OSError:
        return "(лог порожній)"


def _stop_pids(pids: list[int]) -> None:
    for pid in pids:
        if _IS_WIN:
            subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"],
                           capture_output=True, text=True, encoding="utf-8",
                           errors="replace", check=False)
        else:
            try:
                os.kill(pid, 15)
            except OSError:
                pass
    deadline = time.time() + 15
    while time.time() < deadline:
        if not any(_pid_alive(p) for p in pids):
            return
        time.sleep(1)
    for pid in pids:
        if _pid_alive(pid) and _IS_WIN:
            subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"],
                           capture_output=True, text=True, encoding="utf-8",
                           errors="replace", check=False)


def cmd_down(args: argparse.Namespace) -> int:
    pids = _service_pids()
    if pids:
        _log(f"Зупиняю сервіс (PID {', '.join(map(str, pids))})...")
        _stop_pids(pids)
        _log("✓ Сервіс зупинено")
    else:
        _log("Сервіс не працював — нічого зупиняти")
    PIDFILE.unlink(missing_ok=True)

    if args.deep:
        ps = _docker(["ps", "--filter", f"name=^{PG_CONTAINER}$", "--format", "{{.Names}}"])
        if (ps.stdout or "").strip():
            _docker(["stop", PG_CONTAINER])
            _log(f"✓ Контейнер {PG_CONTAINER} зупинено")
        else:
            _log(f"Контейнер {PG_CONTAINER} не запущений")
    else:
        _log(f"(контейнер {PG_CONTAINER} лишився працювати; повна зупинка: down --deep)")
    return 0


def cmd_status(_args: argparse.Namespace) -> int:
    ps = _docker(["ps", "-a", "--filter", f"name=^{PG_CONTAINER}$", "--format", "{{.Names}} {{.Status}}"])
    pg = (ps.stdout or "").strip() or "не знайдено"
    _log(f"PostgreSQL: {pg}")

    pids = _service_pids()
    age = _heartbeat_age_s()
    if age is not None and age <= HEARTBEAT_MAX_AGE_S:
        _log(f"Сервіс: ПРАЦЮЄ (PID {pids[0] if pids else '?'}, heartbeat {age} с тому)")
    elif age is not None:
        _log(f"Сервіс: неактивний (heartbeat {age} с тому) — ймовірно, зупинений")
    else:
        _log("Сервіс: НЕ ПРАЦЮЄ")

    if CATALOGS.exists():
        cats = sorted((p for p in CATALOGS.iterdir() if p.is_dir()), key=lambda p: p.stat().st_mtime)
        if cats:
            _log(f"Останній каталог: {cats[-1]}")
    return 0


# -------------------------------------------------------------------- get


def cmd_get(args: argparse.Namespace) -> int:
    if not HARVESTER.exists():
        _die(f"Не знайдено {HARVESTER} — спершу: python scripts/harvester_ctl.py up")

    age = _heartbeat_age_s()
    if age is None or age > HEARTBEAT_MAX_AGE_S:
        _log("⚠ Сервіс не запущено (heartbeat відсутній). Продовжую: для get потрібна лише БД, "
             "але за алгоритмом спершу виконайте `up`.")

    before = {p.name for p in CATALOGS.glob("catalog_*")} if CATALOGS.exists() else set()

    _log(f'Готую каталог: тема «{args.topic}», до {args.count} джерел (профіль {args.profile})...')
    _log("  крок 1/2: відбір (LLM) + перевірка доступності URL — це найповільніший крок")
    try:
        prep = _run(
            [str(HARVESTER), "curator", "prepare", args.topic,
             "--limit", str(args.count), "--profile", args.profile, "-o", "catalogs"],
            timeout=PREPARE_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired:
        _die(f"curator prepare не встиг за {PREPARE_TIMEOUT_S} с — спробуйте меншу кількість.")

    out = (prep.stdout or "") + (prep.stderr or "")
    folder = _new_catalog_name(out, before)
    if folder is None:
        tail = "\n".join(out.strip().splitlines()[-15:])
        _die("Не вдалося підготувати каталог. Можливі причини: теми немає в БД, "
             "немає кандидатів strict-pass або проблема з LLM/мережею.\n"
             f"--- вивід curator prepare ---\n{tail}")

    catalog_dir = CATALOGS / folder
    json_path = catalog_dir / f"{folder}.json"
    if not json_path.exists():
        _die(f"Каталог {catalog_dir} створено, але JSON не знайдено.")

    # Куди класти файли: у проєкт агента (--dest) або в папку каталогу.
    if args.dest:
        dest = Path(args.dest).expanduser()
        if not dest.is_absolute():
            dest = (Path.cwd() / dest).resolve()
        dest.mkdir(parents=True, exist_ok=True)
        shutil.copy2(json_path, dest / json_path.name)
        note = f"у ваш проєкт: {dest}"
    else:
        dest = catalog_dir
        note = f"у каталог: {dest}"

    _log("  крок 2/2: завантаження PDF (перевірка %PDF- і SHA-256 проти БД)")
    try:
        fetch = _run(
            [str(PYTHON), str(ROOT / "scripts" / "fetch_card_sources.py"),
             "--from-catalog", str(json_path), "--dest", str(dest)],
            timeout=FETCH_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired:
        _die(f"Завантаження PDF не встигло за {FETCH_TIMEOUT_S} с.")

    fetch_out = (fetch.stdout or "") + (fetch.stderr or "")
    m = re.search(r"fetch_done\s+dest=.*?saved=(\d+)\s+skipped=(\d+)", fetch_out)
    if not m:
        tail = "\n".join(fetch_out.strip().splitlines()[-15:])
        _die(f"Завантаження завершилось незрозуміло.\n--- вивід fetch ---\n{tail}")
    saved, skipped = int(m.group(1)), int(m.group(2))

    pdfs = sorted(dest.glob("*.pdf"))
    total_mb = round(sum(p.stat().st_size for p in pdfs) / 1048576.0, 1)

    _log("")
    _log(f"✓ Готово: {saved} джерел ({total_mb} МБ), {note}")
    _log(f"  Папка: {dest}")
    for p in pdfs:
        _log(f"    - {p.name}")
    if saved == 0:
        _die("Жодного PDF не збережено — див. вивід вище (URL недоступні/не PDF).")
    if skipped:
        _log(f"⚠ Пропущено {skipped} посилань — деталі в MANIFEST.csv папки.")
    if saved < args.count:
        _log(f"⚠ Менше за замовлені {args.count}: частина URL недоступна. Можна повторити get — "
             f"відбір буде новим.")
    _log("")
    _log("Наступний крок: попрацюйте з файлами, потім видаліть цю папку (прибирання за собою), "
         "і завершіть: python scripts/harvester_ctl.py down")
    return 0


def _new_catalog_name(output: str, before: set[str]) -> str | None:
    """Якщо curator prepare створив нову папку — повернути її назву."""
    for name in re.findall(r"catalog_\d{8}_\d{6}", output):
        if name not in before:
            return name
    if CATALOGS.exists():
        fresh = [p.name for p in CATALOGS.glob("catalog_*") if p.name not in before]
        if fresh:
            return max(fresh, key=lambda n: (CATALOGS / n).stat().st_mtime)
    return None


def main() -> int:
    # UTF-8 для консолі: без цього rich падає на символах типу ✓ (cp1251)
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass

    ap = argparse.ArgumentParser(
        prog="harvester_ctl",
        description="Керування сервісом Harvester для агентів: up / get / status / down",
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("up", help="Підняти PostgreSQL + сервіс і дочекатися готовності").set_defaults(func=cmd_up)

    g = sub.add_parser("get", help="Отримати джерела за темою (синхронно, 2-5 хв)")
    g.add_argument("--topic", "-t", required=True, help="Тема/дисципліна, напр. 'Економіка'")
    g.add_argument("--count", "-n", type=int, default=10, help="Кількість джерел (типово 10)")
    g.add_argument("--dest", "-d", default=None,
                   help="Куди класти файли (ваш проєкт). Без аргумента — у catalogs/")
    g.add_argument("--profile", "-p", default="strict", choices=["strict", "softier"],
                   help="Профіль фільтрації якості")
    g.set_defaults(func=cmd_get)

    sub.add_parser("status", help="Стан контейнера, сервісу та каталогів").set_defaults(func=cmd_status)

    d = sub.add_parser("down", help="Зупинити сервіс")
    d.add_argument("--deep", action="store_true", help="Також зупинити контейнер PostgreSQL")
    d.set_defaults(func=cmd_down)

    args = ap.parse_args()
    if args.cmd == "get" and not (1 <= args.count <= 500):
        _die("--count має бути від 1 до 500")
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
