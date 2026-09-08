"""Перевірка доступності PDF-джерел."""

from __future__ import annotations

import structlog

from harvester.config import get_settings
from harvester.net.client import get_http_client

logger = structlog.get_logger()


async def check_availability(
    url: str,
    timeout_s: float = 15.0,
) -> tuple[bool, str | None]:
    """Перевірити доступність PDF за URL.

    Returns (available, reason) — reason = None якщо доступно.
    """
    settings = get_settings()
    headers = {
        "User-Agent": settings.http.user_agent,
        "Accept": "application/pdf,*/*",
    }

    try:
        client = await get_http_client()
        resp = await client.head(url, headers=headers, timeout=timeout_s)
        if resp.status_code == 405:
            resp = await client.get(url, headers=headers, timeout=timeout_s)
        if not 200 <= resp.status_code < 300:
            return False, f"HTTP {resp.status_code}"
        ct = resp.headers.get("content-type", "").lower()
        if ct and "pdf" not in ct and "octet-stream" not in ct and not url.lower().split("?", 1)[0].endswith(".pdf"):
            return False, f"не PDF (content-type={ct})"
        return True, None
    except TimeoutError:
        return False, "connect_timeout"
    except OSError:
        return False, "connect_error"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"
