"""Перевірка доступності PDF-джерел."""

from __future__ import annotations

import structlog

from harvester.config import get_settings
from harvester.net.client import get_http_client

logger = structlog.get_logger()


async def check_availability(
    url: str,
    timeout_s: float = 15.0,
    log_rejection: bool = True,
) -> tuple[bool, str | None]:
    """Перевірити доступність PDF за URL.

    Returns (available, reason) — reason = None якщо доступно.

    Кожна відмова логується: раніше прогін куратора міг відхилити тисячі
    кандидатів, не залишивши жодного сліду — не було видно, чи це очікувана
    фільтрація (SEO-сміття) чи масова поломка мережі/доступності.
    """
    settings = get_settings()
    headers = {
        "User-Agent": settings.http.user_agent,
        "Accept": "application/pdf,*/*",
    }

    def _reject(reason: str) -> tuple[bool, str]:
        if log_rejection:
            logger.debug("availability_rejected", url=url[:200], reason=reason)
        return False, reason

    try:
        client = await get_http_client()
        resp = await client.head(url, headers=headers, timeout=timeout_s)
        if resp.status_code == 405:
            # HEAD не підтримується — пробуємо GET.
            logger.debug("availability_head_unsupported", url=url[:200])
            resp = await client.get(url, headers=headers, timeout=timeout_s)
        if not 200 <= resp.status_code < 300:
            return _reject(f"HTTP {resp.status_code}")
        ct = resp.headers.get("content-type", "").lower()
        if ct and "pdf" not in ct and "octet-stream" not in ct and not url.lower().split("?", 1)[0].endswith(".pdf"):
            return _reject(f"не PDF (content-type={ct})")
        return True, None
    except TimeoutError:
        return _reject("connect_timeout")
    except OSError:
        return _reject("connect_error")
    except Exception as e:
        # Раніше помилка перетворювалась на reason без запису в журнал;
        # тип винятку — єдина підказка, чи це наш баг, чи відмова сервера.
        logger.warning(
            "availability_check_error",
            url=url[:200],
            error=str(e)[:200],
            error_type=type(e).__name__,
        )
        return False, f"{type(e).__name__}: {e}"
