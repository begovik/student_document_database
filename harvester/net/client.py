import asyncio
import hashlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import os
from pathlib import Path
from urllib.parse import urljoin, urlparse

import httpx
import structlog

from harvester.config import get_settings
from harvester.core.ratelimit import BandwidthLimiter, GlobalRateLimiter, HostRateLimiter

logger = structlog.get_logger()


class HttpClient:
    _instance: "HttpClient | None" = None
    _instance_lock = asyncio.Lock()

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
        async with cls._instance_lock:
            if cls._instance is None:
                instance = cls()
                await instance.initialize()
                cls._instance = instance
        return cls._instance

    @classmethod
    async def reset(cls) -> None:
        async with cls._instance_lock:
            if cls._instance is not None:
                await cls._instance.close()
                cls._instance = None

    async def initialize(self) -> None:
        if self._client is not None:
            return

        self._client = httpx.AsyncClient(
            http2=True,
            # Redirects проходять через ручну перевірку SSRF у request/stream.
            follow_redirects=False,
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

        kwargs = dict(kwargs)
        kwargs.pop("follow_redirects", None)
        max_redirects = int(kwargs.pop("max_redirects", self.settings.http.max_redirects))
        current_url = url
        current_method = method

        await self.global_limiter.acquire()
        try:
            for redirect_count in range(max_redirects + 1):
                await self._assert_url_allowed(current_url)
                host = urlparse(current_url).hostname or ""
                await self.host_limiter.wait(host)
                response = await self._client.request(
                    current_method,
                    current_url,
                    follow_redirects=False,
                    **kwargs,
                )
                location = response.headers.get("location")
                if response.status_code not in (301, 302, 303, 307, 308) or not location:
                    return response
                if redirect_count >= max_redirects:
                    await response.aclose()
                    raise httpx.TooManyRedirects(
                        f"Забагато перенаправлень для {url}", request=response.request
                    )

                next_url = urljoin(str(response.url), location)
                await response.aclose()
                if response.status_code == 303 or (
                    response.status_code in (301, 302)
                    and current_method.upper() not in ("GET", "HEAD")
                ):
                    current_method = "GET"
                    for key in ("content", "data", "json"):
                        kwargs.pop(key, None)
                current_url = next_url
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

        kwargs = dict(kwargs)
        kwargs.pop("follow_redirects", None)
        max_redirects = int(kwargs.pop("max_redirects", self.settings.http.max_redirects))
        current_url = url
        current_method = method
        context = None

        await self.global_limiter.acquire()
        try:
            for redirect_count in range(max_redirects + 1):
                await self._assert_url_allowed(current_url)
                host = urlparse(current_url).hostname or ""
                await self.host_limiter.wait(host)
                context = self._client.stream(
                    current_method,
                    current_url,
                    follow_redirects=False,
                    **kwargs,
                )
                response = await context.__aenter__()
                location = response.headers.get("location")
                if response.status_code not in (301, 302, 303, 307, 308) or not location:
                    yield response
                    return
                if redirect_count >= max_redirects:
                    raise httpx.TooManyRedirects(
                        f"Забагато перенаправлень для {url}", request=response.request
                    )

                next_url = urljoin(str(response.url), location)
                await context.__aexit__(None, None, None)
                context = None
                if response.status_code == 303 or (
                    response.status_code in (301, 302)
                    and current_method.upper() not in ("GET", "HEAD")
                ):
                    current_method = "GET"
                    for key in ("content", "data", "json"):
                        kwargs.pop(key, None)
                current_url = next_url
        finally:
            if context is not None:
                await context.__aexit__(None, None, None)
            self.global_limiter.release()

    async def _assert_url_allowed(self, url: str) -> None:
        from harvester.net.guards import is_url_allowed

        allowed, reason = await is_url_allowed(url)
        if not allowed:
            raise httpx.InvalidURL(f"URL заборонено ({reason}): {url}")

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

    async def stream_to_file(
        self,
        url: str,
        destination: Path,
        max_bytes: int | None = None,
    ) -> tuple[int, str, bytes]:
        """Потоково записати відповідь у temp-файл без накопичення всього PDF у RAM."""
        if max_bytes is None:
            max_bytes = self.settings.http.max_pdf_bytes

        hasher = hashlib.sha256()
        prefix = bytearray()
        total_bytes = 0
        fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            async with self.stream("GET", url) as response:
                response.raise_for_status()
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    total_bytes += len(chunk)
                    if total_bytes > max_bytes:
                        raise ValueError(f"Download exceeds max_bytes: {max_bytes}")
                    await self.bandwidth_limiter.wait_for_bytes(len(chunk))
                    hasher.update(chunk)
                    if len(prefix) < 5:
                        prefix.extend(chunk[: 5 - len(prefix)])
                    await asyncio.to_thread(_write_all, fd, chunk)
        finally:
            os.close(fd)

        return total_bytes, hasher.hexdigest(), bytes(prefix)


def _write_all(fd: int, data: bytes) -> None:
    """Повністю записати chunk у файл, обробляючи можливий partial write."""
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        view = view[written:]


async def get_http_client() -> HttpClient:
    return await HttpClient.get_instance()
