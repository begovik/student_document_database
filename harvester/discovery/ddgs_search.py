import asyncio
import random
from collections.abc import AsyncIterator

import structlog
from ddgs import DDGS
from ddgs.exceptions import DDGSException, RatelimitException, TimeoutException

from harvester.config import get_settings
from harvester.discovery.base import Candidate
from harvester.net.guards import is_url_allowed, validate_url_format

logger = structlog.get_logger()

MAX_BACKENDS_PER_QUERY = 3


class DDGSSearchError(RuntimeError):
    """Помилка всіх backend-ів пошуку, яку не можна вважати порожнім результатом."""


# Точний текст, який ddgs піднімає, коли ВСІ engine-и відпрацювали без
# помилки, але нічого не знайшли (ddgs/ddgs.py:223 — `err or "No results
# found."`). Звіряння за повним рядком, а не за підрядком: інакше реальна
# помилка engine-а на кшталт "no results attribute" була б прийнята за
# легітимний порожній результат, і такий збій став би невидимим.
_NO_RESULTS_MESSAGE = "no results found."


def _is_no_results(exc: BaseException) -> bool:
    """Чи означає виняток ddgs порожній результат, а не збій backend-a.

    Точний збіг, а не підрядок: повідомлення про помилку engine-а може
    містити слова «no results» у складі іншої фрази, і такий збій не
    можна вважати нормою.
    """
    return str(exc).strip().lower() == _NO_RESULTS_MESSAGE


class DDGSSearchChannel:
    name = "ddgs"

    def __init__(self):
        settings = get_settings()
        self.enabled = settings.channels.ddgs.enabled
        self.backends = settings.channels.ddgs.backends
        self.query_interval = settings.channels.ddgs.query_interval_s
        self._current_backend_idx = 0

    def rate_limit(self) -> float:
        return (self.query_interval[0] + self.query_interval[1]) / 2

    def _get_next_backend(self) -> str:
        backend = self.backends[self._current_backend_idx % len(self.backends)]
        self._current_backend_idx += 1
        return backend

    async def discover(self, task: dict) -> AsyncIterator[Candidate]:
        if not self.enabled:
            return

        query_text = task.get("query_text")
        region = task.get("region", "ua-uk")
        max_results = task.get("max_results", 30)

        if not query_text:
            logger.warning("ddgs_no_query_text", task=task)
            raise ValueError("DDGS search task must contain query_text")

        if not self.backends:
            raise DDGSSearchError("DDGS не налаштовано жодного backend-а")

        results: list[dict] = []
        backends_tried: list[str] = []
        rate_limited = False
        errors: list[str] = []
        empty_backends: list[str] = []

        for _ in range(min(MAX_BACKENDS_PER_QUERY, len(self.backends))):
            backend = self._get_next_backend()
            backends_tried.append(backend)
            try:
                results = await asyncio.to_thread(
                    self._search_sync, query_text, backend, region, max_results
                )
                if results:
                    break
                logger.debug("ddgs_empty", query=query_text, backend=backend)
            except RatelimitException as e:
                rate_limited = True
                errors.append(f"{backend}: rate limit ({e})")
                logger.warning("ddgs_ratelimit", query=query_text, backend=backend, error=str(e))
                continue
            except TimeoutException as e:
                errors.append(f"{backend}: timeout ({e})")
                logger.warning("ddgs_timeout", query=query_text, backend=backend, error=str(e))
                continue
            except DDGSException as e:
                # Бібліотека ddgs піднімає DDGSException("No results found.")
                # коли ВСІ engine-и відпрацювали без помилки, але нічого не
                # знайшли. Це-legitимний порожній результат, а не збій
                # backend-а (див. ddgs/ddgs.py:220-223: `err` порожній →
                # raise DDGSException("No results found.")).
                #
                # Раніше такий випадок рахувався помилкою: запит отримував
                # 30-хв error-cooldown замість zero_streak-сходинки, а кожна
                # спроба писала 2 error-події з повним traceback. На пулі
                # з 4 000 запитів це глушило реальні збої пошуку.
                if _is_no_results(e):
                    empty_backends.append(backend)
                    logger.debug(
                        "ddgs_backend_no_results", query=query_text, backend=backend
                    )
                    continue
                errors.append(f"{backend}: {e}")
                logger.warning("ddgs_backend_error", query=query_text, backend=backend, error=str(e))
                continue
            except Exception as e:  # noqa: BLE001
                # Не-DDGSException (напр. зміна бібліотеки, проблема з
                # DNS/проксі) інакше виривав би з discover() поза обробкою
                # і виглядав би як збій воркера, а не помилка backend-а.
                errors.append(f"{backend}: неочікувана помилка {type(e).__name__}: {e}")
                logger.warning(
                    "ddgs_backend_unexpected_error",
                    query=query_text,
                    backend=backend,
                    error=str(e)[:200],
                    error_type=type(e).__name__,
                )
                continue

        logger.info(
            "ddgs_search_complete",
            query=query_text,
            results=len(results),
            backends=backends_tried,
            rate_limited=rate_limited,
            errors=len(errors),
            empty_backends=len(empty_backends),
        )

        # Помилка ставиться лише коли результату немає І був справжній
        # збій (rate-limit/timeout/помилка engine). Якщо всі backend-и
        # відпрацювали чисто, але нічого не знайшли — це валідний нульовий
        # результат: запит отримує zero_streak-сходинку, а не error-cooldown.
        if not results and (errors or rate_limited):
            detail = "; ".join(errors) or "усі backend-и rate-limited"
            raise DDGSSearchError(
                f"DDGS не отримав результатів для запиту ({'; '.join(backends_tried)}): {detail}"
            )

        if not results and empty_backends:
            logger.info(
                "ddgs_all_backends_empty",
                query=query_text,
                backends=empty_backends,
            )

        yielded = 0
        dropped_invalid = 0
        dropped_blocked: dict[str, int] = {}

        for result in results:
            href = result.get("href")
            if not href or not validate_url_format(href):
                # Раніше `continue` без логу: движок пошуку повертає рекламні
                # та технічні URL, тому незрозуміло було, чому з 30 «результатів»
                # реєструється 3 документи.
                dropped_invalid += 1
                logger.debug(
                    "ddgs_result_invalid",
                    query=query_text,
                    href=str(href)[:150],
                    reason="empty" if not href else "bad_format",
                )
                continue

            allowed, reason = await is_url_allowed(href)
            if not allowed:
                dropped_blocked[reason or "unknown"] = dropped_blocked.get(reason or "unknown", 0) + 1
                logger.debug("ddgs_url_blocked", url=href[:150], reason=reason, query=query_text)
                continue

            title = result.get("title")
            body = result.get("body")

            yielded += 1
            yield Candidate(
                url=href,
                title_hint=title,
                channel=self.name,
                query_text=query_text,
                ref_url=href,
                extra={"body": body, "backends": backends_tried},
            )

        # Підсумок фільтрації: без нього втрата кандидатів невидима.
        if dropped_invalid or dropped_blocked:
            logger.info(
                "ddgs_results_filtered",
                query=query_text[:100],
                results=len(results),
                yielded=yielded,
                dropped_invalid=dropped_invalid,
                dropped_blocked=dropped_blocked,
            )

    def _search_sync(
        self,
        query: str,
        backend: str,
        region: str,
        max_results: int,
    ) -> list[dict]:
        ddgs = DDGS()
        results = ddgs.text(
            query,
            region=region,
            safesearch="off",
            backend=backend,
            max_results=max_results,
        )
        return list(results) if results else []

    async def wait_interval(self) -> None:
        delay = random.uniform(*self.query_interval)
        await asyncio.sleep(delay)
