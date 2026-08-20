import asyncio
import time
from enum import Enum
from typing import Callable

import structlog

logger = structlog.get_logger()


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 300.0,
        max_recovery_timeout: float = 21600.0,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.max_recovery_timeout = max_recovery_timeout
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = 0.0
        self._current_timeout = recovery_timeout
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        return self._state

    async def call(self, func: Callable, *args, **kwargs):
        async with self._lock:
            if self._state == CircuitState.OPEN:
                if time.monotonic() - self._last_failure_time >= self._current_timeout:
                    self._state = CircuitState.HALF_OPEN
                    logger.debug("circuit_breaker_half_open")
                else:
                    raise CircuitBreakerOpenError("Circuit breaker is open")

        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                await self._on_success()
            return result
        except Exception as e:
            async with self._lock:
                await self._on_failure()
            raise

    async def _on_success(self) -> None:
        if self._state == CircuitState.HALF_OPEN:
            logger.info("circuit_breaker_closed_after_half_open")
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._current_timeout = self.recovery_timeout

    async def _on_failure(self) -> None:
        self._failure_count += 1
        self._last_failure_time = time.monotonic()

        if self._failure_count >= self.failure_threshold:
            self._state = CircuitState.OPEN
            logger.warning(
                "circuit_breaker_opened",
                failure_count=self._failure_count,
                timeout=self._current_timeout,
            )
            self._current_timeout = min(self._current_timeout * 2, self.max_recovery_timeout)

    async def reset(self) -> None:
        async with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._current_timeout = self.recovery_timeout


class CircuitBreakerOpenError(Exception):
    pass


class CircuitBreakerRegistry:
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 300.0,
        max_recovery_timeout: float = 21600.0,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.max_recovery_timeout = max_recovery_timeout
        self._breakers: dict[str, CircuitBreaker] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> CircuitBreaker:
        async with self._lock:
            if key not in self._breakers:
                self._breakers[key] = CircuitBreaker(
                    self.failure_threshold,
                    self.recovery_timeout,
                    self.max_recovery_timeout,
                )
            return self._breakers[key]

    async def get_all_states(self) -> dict[str, str]:
        async with self._lock:
            return {key: breaker.state.value for key, breaker in self._breakers.items()}
