"""Тести для скрипта відкладеної перевірки класифікації.

Перевіряється:
- стан зберігається і читається;
- лист надсилається тільки коли робота завершилась або не просувається;
- наступна перевірка планується, поки залиш ненульовий, і не планується
  після завершення.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "check_classify_done.py"
_spec = importlib.util.spec_from_file_location("check_classify_done", _SCRIPT)
assert _spec and _spec.loader
mod = importlib.util.module_from_spec(_spec)
sys.modules["check_classify_done"] = mod
_spec.loader.exec_module(mod)


@pytest.fixture
def script(tmp_path, monkeypatch):
    """Скрипт із підміняними БД, поштою та планувальником."""
    sent: list[tuple[str, str]] = []
    scheduled: list[float] = []

    async def _count_backlog() -> int:
        return int(getattr(script, "_backlog", 0))

    async def _send(subject: str, body: str, key=None, interval=None) -> bool:
        sent.append((subject, body))
        return True

    def _schedule(interval_h: float, state_path: str) -> str:
        scheduled.append(interval_h)
        return "job 1"

    monkeypatch.setattr(mod, "count_backlog", _count_backlog)
    monkeypatch.setattr(mod, "send_notification", _send)
    monkeypatch.setattr(mod, "schedule_next", _schedule)
    script.sent = sent
    script.scheduled = scheduled
    script.state_path = str(tmp_path / "state.json")
    return script


async def _run(script, extra_args=None):
    args = ["--state", script.state_path] + (extra_args or [])
    sys.argv = ["check_classify_done.py", *args]
    return await mod.main()


def _read_state(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


@pytest.mark.asyncio
async def test_state_is_persisted(script):
    script._backlog = 500
    await _run(script, ["--no-schedule"])

    state = await asyncio.to_thread(_read_state, script.state_path)
    assert state["last_backlog"] == 500
    assert state["last_service_state"]
    assert script.sent == []


@pytest.mark.asyncio
async def test_no_email_while_progressing(script):
    """Поки є прогрес — лист не надсилається, наступну перевірку плануємо."""
    script._backlog = 500
    await _run(script, ["--no-schedule"])

    script._backlog = 400
    await _run(script)

    assert script.sent == []
    assert script.scheduled == [12.0]


@pytest.mark.asyncio
async def test_email_when_finished_and_no_further_scheduling(script):
    """Завершення — лист і жодної наступної перевірки."""
    script._backlog = 0
    await _run(script)

    assert len(script.sent) == 1
    assert "завершена" in script.sent[0][0]
    assert script.scheduled == []


@pytest.mark.asyncio
async def test_email_when_stalled(script):
    """Залиш не зменшився — це подія, про яку треба повідомити."""
    script._backlog = 500
    await _run(script, ["--no-schedule"])

    script._backlog = 500
    await _run(script)

    assert len(script.sent) == 1
    assert "не просувається" in script.sent[0][0]
    assert script.scheduled == [12.0]


@pytest.mark.asyncio
async def test_start_at_guard_skips_early_run(script):
    """До заданого моменту перевірка не робить нічого."""
    script._backlog = 500
    await _run(script, ["--no-schedule", "--start-at", "2099-01-01T00:00:00"])

    assert script.sent == []
    assert script.scheduled == []
    assert not Path(script.state_path).exists()
