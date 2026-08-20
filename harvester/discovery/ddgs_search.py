import asyncio
import random
from typing import AsyncIterator

import structlog
from ddgs import DDGS
from ddgs.exceptions import DDGSException, RatelimitException, TimeoutException

from harvester.config import get_settings
from harvester.discovery.base import Candidate
from harvester.net.guards import is_url_allowed, validate_url_format

logger = structlog.get_logger()

MAX_BACKENDS_PER_QUERY = 3


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
            return

        results: list[dict] = []
        backends_tried: list[str] = []
        rate_limited = False

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
            except RatelimitException:
                rate_limited = True
                logger.warning("ddgs_ratelimit", query=query_text, backend=backend)
                continue
            except TimeoutException as e:
                logger.warning("ddgs_timeout", query=query_text, backend=backend, error=str(e))
                continue
            except DDGSException as e:
                logger.warning("ddgs_backend_error", query=query_text, backend=backend, error=str(e))
                continue

        logger.info(
            "ddgs_search_complete",
            query=query_text,
            results=len(results),
            backends=backends_tried,
            rate_limited=rate_limited,
        )

        if rate_limited and not results:
            await asyncio.sleep(60)

        for result in results:
            href = result.get("href")
            if not href or not validate_url_format(href):
                continue

            allowed, reason = await is_url_allowed(href)
            if not allowed:
                logger.debug("ddgs_url_blocked", url=href, reason=reason)
                continue

            title = result.get("title")
            body = result.get("body")

            yield Candidate(
                url=href,
                title_hint=title,
                channel=self.name,
                query_text=query_text,
                ref_url=href,
                extra={"body": body, "backends": backends_tried},
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
