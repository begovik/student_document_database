"""Тимчасові PDF не повинні залишатися на диску.

`data/tmp` не сховище: PDF потрібен лише на час верифікації. Регресія
була в `_step_download` — файл прибирали тільки в `except`, а гілки
`TOO_SMALL` і `NOT_PDF` поверталися звичайним return, залишаючи файл
назавжди (виміряно: 110 із 131 файлу в data/tmp).
"""

import asyncio
import types

import pytest

from harvester.config import Settings, reload_settings
from harvester.verify.pipeline import VerifyPipeline


class _FakeHttp:
    """Віддає заданий «зміст» і пише його у файл, як робить справжній клієнт."""

    def __init__(self, payload: bytes):
        self.payload = payload

    async def stream_to_file(self, url, dest, max_bytes, *args, **kwargs):
        dest.write_bytes(self.payload)
        return len(self.payload), "a" * 64, self.payload[:8]


def _pipeline(tmp_path, payload: bytes) -> VerifyPipeline:
    settings = reload_settings()
    settings.paths.tmp_dir = str(tmp_path)
    pipe = VerifyPipeline(db=types.SimpleNamespace(), http_client=_FakeHttp(payload))
    pipe._log_attempt = _noop
    return pipe


async def _noop(*args, **kwargs):
    return None


def _tmp_files(tmp_path):
    return list(tmp_path.glob("verify_*.pdf"))


@pytest.mark.parametrize(
    "payload,code",
    [
        # Порівняння за розміром іде перед перевіркою сигнатури, тож
        # для гілки NOT_PDF зміст має бути більшим за min_pdf_bytes.
        (b"<html>404</html>" + b"x" * 20000, "NOT_PDF"),
        (b"%PDF-1.7 tiny", "TOO_SMALL"),
    ],
)
def test_rejected_download_leaves_no_file(tmp_path, payload, code):
    """Відхилений файл не має залишатися — інакше диск повільно забивається."""
    pipe = _pipeline(tmp_path, payload)

    result = asyncio.run(
        pipe._step_download("https://example.org/a", 1, "2026-01-01T00:00:00")
    )

    assert result.code == code
    assert _tmp_files(tmp_path) == [], "файл мав бути прибраний"


def test_successful_download_keeps_file_for_caller(tmp_path):
    """На успіху файл лишається: його розбирає й видаляє викликач."""
    pipe = _pipeline(tmp_path, b"%PDF-1.7 " + b"x" * 20000)

    result = asyncio.run(
        pipe._step_download("https://example.org/a", 1, "2026-01-01T00:00:00")
    )

    assert result.success
    assert len(_tmp_files(tmp_path)) == 1

    path, size, sha = result.message.split("|")
    from pathlib import Path

    Path(path).unlink()
    assert _tmp_files(tmp_path) == []
    assert int(size) > 0 and len(sha) == 64


def test_reload_settings_returns_settings():
    assert isinstance(reload_settings(), Settings)


def test_no_tmp_leftovers_over_many_rejections(tmp_path):
    """Найважливіше: втечі накопичуються мільйонами, а не по одній."""
    pipe = _pipeline(tmp_path, b"<html>" + b"x" * 20000)
    for _ in range(25):
        asyncio.run(
            pipe._step_download("https://example.org/a", 1, "2026-01-01T00:00:00")
        )
    assert _tmp_files(tmp_path) == []