import asyncio
import time

import structlog

logger = structlog.get_logger()


class TokenBucket:
    def __init__(self, rate: float, burst: int = 1):
        self.rate = rate
        self.burst = burst
        self.tokens = float(burst)
        self.last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self.last_refill
                self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
                self.last_refill = now

                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                wait_time = (1 - self.tokens) / self.rate
            await asyncio.sleep(wait_time)


class HostRateLimiter:
    def __init__(self, default_delay_ms: int = 2000, default_burst: int = 2):
        self.default_delay_ms = default_delay_ms
        self.default_burst = default_burst
        self._buckets: dict[str, TokenBucket] = {}
        self._lock = asyncio.Lock()

    async def wait(self, host: str) -> None:
        async with self._lock:
            if host not in self._buckets:
                rate = 1000.0 / self.default_delay_ms
                self._buckets[host] = TokenBucket(rate, self.default_burst)
            bucket = self._buckets[host]

        await bucket.acquire()

    def set_delay(self, host: str, delay_ms: int, burst: int = 2) -> None:
        rate = 1000.0 / delay_ms
        self._buckets[host] = TokenBucket(rate, burst)


class GlobalRateLimiter:
    def __init__(self, max_concurrent: int = 32):
        self._semaphore = asyncio.Semaphore(max_concurrent)

    async def acquire(self) -> None:
        await self._semaphore.acquire()

    def release(self) -> None:
        self._semaphore.release()


class BandwidthLimiter:
    def __init__(self, max_bytes_per_second: float = 2_000_000):
        self.max_bps = max_bytes_per_second
        self._tokens = max_bytes_per_second
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def wait_for_bytes(self, byte_count: int) -> None:
        if byte_count <= 0:
            return
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_refill
                self._tokens = min(self.max_bps, self._tokens + elapsed * self.max_bps)
                self._last_refill = now

                if self._tokens >= byte_count:
                    self._tokens -= byte_count
                    return
                # Один HTTP chunk може бути більшим за bucket capacity.
                if byte_count > self.max_bps:
                    wait_time = max(0.0, (byte_count - self._tokens) / self.max_bps)
                    self._tokens = 0
                    self._last_refill = now + wait_time
                    consume_entire_chunk = True
                else:
                    wait_time = (byte_count - self._tokens) / self.max_bps
                    consume_entire_chunk = False
            await asyncio.sleep(wait_time)
            if consume_entire_chunk:
                return
