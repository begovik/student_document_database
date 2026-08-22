"""Перевірка доступності PDF-джерел."""

from __future__ import annotations

import httpx
import structlog

from harvester.config import get_settings

logger = structlog.get_logger()


async def check_availability(
    url: str,
    timeout_s: float = 15.0,
) -> tuple[bool, str | None]:
    """Перевірити доступність PDF за URL.

    Returns (available, reason) — reason = None якщо доступно.
    """
    settings = get_settings()
    timeout = httpx.Timeout(timeout_s, connect=5.0, read=5.0, pool=None)
    headers = {
        "User-Agent": settings.http.user_agent,
        "Accept": "application/pdf,*/*",
    }

    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            resp = await client.head(url, headers=headers)
            if resp.status_code != 200:
                return False, f"HTTP {resp.status_code}"
            ct = resp.headers.get("content-type", "").lower()
            if "pdf" not in ct and "octet-stream" not in ct:
                return False, f"не PDF (content-type={ct})"
            return True, None
    except httpx.ConnectTimeout:
        return False, "connect_timeout"
    except httpx.ReadTimeout:
        return False, "read_timeout"
    except httpx.ConnectError:
        return False, "connect_error"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"
