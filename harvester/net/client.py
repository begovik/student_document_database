import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator

import httpx
import structlog

from harvester.config import get_settings
from harvester.core.ratelimit import BandwidthLimiter, GlobalRateLimiter, HostRateLimiter

logger = structlog.get_logger()


class HttpClient:
    _instance: "HttpClient | None" = None

    def __init__(self):
        settings = get_settings()
        self.settings = settings
        self._client: httpx.AsyncClient | None = None
        self.global_limiter = GlobalRateLimiter(settings.http.global_concurrency)
        self.host_limiter = HostRateLimiter(
            settings.http.per_host_delay_ms,
            settings.http.per_host_burst,
        )
        self.bandwidth_limiter = BandwidthLimiter(
            settings.http.bandwidth_mbps * 1_000_000 / 8
        )

    @classmethod
    async def get_instance(cls) -> "HttpClient":
        if cls._instance is None:
            cls._instance = cls()
            await cls._instance.initialize()
        return cls._instance

    @classmethod
    async def reset(cls) -> None:
        if cls._instance is not None:
            await cls._instance.close()
            cls._instance = None

    async def initialize(self) -> None:
        if self._client is not None:
            return

        self._client = httpx.AsyncClient(
            http2=True,
            follow_redirects=True,
            max_redirects=self.settings.http.max_redirects,
            timeout=httpx.Timeout(
                connect=self.settings.http.connect_timeout_s,
                read=self.settings.http.read_timeout_s,
                write=self.settings.http.read_timeout_s,
                pool=self.settings.http.connect_timeout_s,
            ),
            headers={
                "User-Agent": self.settings.http.user_agent,
                "Accept": "*/*",
                "Accept-Language": "uk,en;q=0.9",
            },
        )
        logger.info("http_client_initialized")

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.info("http_client_closed")

    async def request(
        self,
        method: str,
        url: str,
        **kwargs,
    ) -> httpx.Response:
        if not self._client:
            raise RuntimeError("HttpClient not initialized")

        from urllib.parse import urlparse
        host = urlparse(url).netloc.split(":")[0]

        await self.global_limiter.acquire()
        try:
            await self.host_limiter.wait(host)

            response = await self._client.request(method, url, **kwargs)
            return response
        finally:
            self.global_limiter.release()

    async def get(self, url: str, **kwargs) -> httpx.Response:
        return await self.request("GET", url, **kwargs)

    async def head(self, url: str, **kwargs) -> httpx.Response:
        return await self.request("HEAD", url, **kwargs)

    @asynccontextmanager
    async def stream(self, method: str, url: str, **kwargs) -> AsyncIterator[httpx.Response]:
        if not self._client:
            raise RuntimeError("HttpClient not initialized")

        from urllib.parse import urlparse
        host = urlparse(url).netloc.split(":")[0]

        await self.global_limiter.acquire()
        try:
            await self.host_limiter.wait(host)

            async with self._client.stream(method, url, **kwargs) as response:
                yield response
        finally:
            self.global_limiter.release()

    async def stream_download(
        self,
        url: str,
        max_bytes: int | None = None,
    ) -> tuple[bytes, int]:
        if max_bytes is None:
            max_bytes = self.settings.http.max_pdf_bytes

        chunks = []
        total_bytes = 0

        async with self.stream("GET", url) as response:
            response.raise_for_status()

            async for chunk in response.aiter_bytes(chunk_size=65536):
                total_bytes += len(chunk)
                if total_bytes > max_bytes:
                    raise ValueError(f"Download exceeds max_bytes: {max_bytes}")

                await self.bandwidth_limiter.wait_for_bytes(len(chunk))
                chunks.append(chunk)

        return b"".join(chunks), total_bytes


async def get_http_client() -> HttpClient:
    return await HttpClient.get_instance()
