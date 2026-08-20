import asyncio
import time
from dataclasses import dataclass

import httpx
import structlog

from harvester.config import get_settings

logger = structlog.get_logger()


class LLMUnavailable(Exception):
    """Усі LLM-провайдери недоступні."""

    pass


class AllLimitsExhausted(Exception):
    """Усі ключі та моделі вичерпали денні ліміти."""

    pass


@dataclass
class LLMResponse:
    text: str
    provider: str
    model: str
    duration_ms: int


class LLMClient:
    """
    Gemini з ротацією моделей та ключів.
    
    Логіка:
    - Дві моделі: gemini-3.1-flash-lite, gemini-3.5-flash-lite
    - Три ключі: GEMINI_API_KEY, GEMINI_API_KEY_2, GEMINI_API_KEY_3
    - Якщо модель недоступна > 2 хвилин → денний ліміт → наступна модель
    - Якщо обидві моделі вичерпали → наступний ключ
    - Якщо всі ключі та моделі вичерпали → AllLimitsExhausted
    """

    def __init__(self):
        self.settings = get_settings()
        self._models = self.settings.llm.gemini_models
        self._keys = self.settings.gemini_keys
        self._model_idx = 0
        self._key_idx = 0
        self._last_call = 0.0
        self._lock = asyncio.Lock()
        self._daily_limit_exhausted: set[tuple[int, int]] = set()
        self._initialized = False

    @property
    def enabled(self) -> bool:
        return self.settings.llm.enabled and bool(
            self._keys or self.settings.open_router_api_key
        )

    async def initialize(self) -> None:
        """Перевіряє першу модель при запуску, пропускає вичерпані."""
        if self._initialized:
            return

        self._initialized = True

        if not self._keys:
            return

        for key_idx in range(len(self._keys)):
            for model_idx in range(len(self._models)):
                key = self._keys[(self._key_idx + key_idx) % len(self._keys)]
                model = self._models[(self._model_idx + model_idx) % len(self._models)]
                
                available = await self._check_model_available(key, model)
                if available:
                    self._key_idx = (self._key_idx + key_idx) % len(self._keys)
                    self._model_idx = (self._model_idx + model_idx) % len(self._models)
                    logger.info(
                        "llm_initialized",
                        key_idx=self._key_idx,
                        model=self._models[self._model_idx],
                    )
                    return
                else:
                    logger.warning(
                        "llm_model_exhausted_at_startup",
                        key_idx=(self._key_idx + key_idx) % len(self._keys),
                        model=model,
                    )

        logger.critical("llm_all_limits_exhausted_at_startup")
        raise AllLimitsExhausted("Усі ключі та моделі вичерпали денні ліміти")

    async def _check_model_available(self, key: str, model: str) -> bool:
        """Перевіряє доступність моделі з коротким запитом."""
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(
                    f"{self.settings.llm.gemini_base_url}/models/{model}:generateContent",
                    params={"key": key},
                    json={
                        "contents": [{"parts": [{"text": "1+1"}]}],
                        "generationConfig": {"maxOutputTokens": 10},
                    },
                )
                if resp.status_code == 429:
                    if "quota" in resp.text.lower() or "exceeded" in resp.text.lower():
                        return False
                if resp.status_code in (401, 403):
                    return False
                return True
        except Exception:
            return False

    async def complete(self, prompt: str) -> LLMResponse:
        if not self.enabled:
            raise LLMUnavailable("LLM вимкнено або немає ключів")

        await self.initialize()
        await self._throttle()

        if not self._keys:
            if self.settings.open_router_api_key:
                try:
                    return await self._call_openrouter(prompt)
                except Exception as e:
                    raise LLMUnavailable(f"openrouter error: {e}")
            raise LLMUnavailable("Немає Gemini ключів")

        errors: list[str] = []
        start_key_idx = self._key_idx
        start_model_idx = self._model_idx
        checked_all = False

        while not checked_all:
            key = self._keys[self._key_idx]
            model = self._models[self._model_idx]

            if (self._key_idx, self._model_idx) in self._daily_limit_exhausted:
                self._advance()
                if self._is_back_to_start(start_key_idx, start_model_idx):
                    checked_all = True
                continue

            try:
                result = await self._call_gemini_with_wait(prompt, key, model)
                return result
            except GeminiQuotaExceeded as e:
                logger.warning(
                    "gemini_quota_exceeded",
                    key_idx=self._key_idx,
                    model=model,
                    waiting_s=self.settings.llm.daily_limit_wait_s,
                )
                await asyncio.sleep(self.settings.llm.daily_limit_wait_s)
                
                available = await self._check_model_available(key, model)
                if not available:
                    logger.warning(
                        "gemini_daily_limit_confirmed",
                        key_idx=self._key_idx,
                        model=model,
                    )
                    self._daily_limit_exhausted.add((self._key_idx, self._model_idx))
                    errors.append(f"{model}[key{self._key_idx}]: daily limit")
                    self._advance()
                    if self._is_back_to_start(start_key_idx, start_model_idx):
                        checked_all = True
                else:
                    logger.info("gemini_quota_recovered", key_idx=self._key_idx, model=model)
            except GeminiRateLimited as e:
                logger.warning("gemini_rate_limited", key_idx=self._key_idx, model=model)
                errors.append(str(e))
                await asyncio.sleep(2)
            except GeminiAuthError as e:
                logger.error("gemini_auth_error", key_idx=self._key_idx, model=model, error=str(e))
                errors.append(str(e))
                self._daily_limit_exhausted.add((self._key_idx, self._model_idx))
                self._advance()
                if self._is_back_to_start(start_key_idx, start_model_idx):
                    checked_all = True
            except Exception as e:
                logger.error("gemini_error", key_idx=self._key_idx, model=model, error=str(e))
                errors.append(str(e))
                self._advance()
                if self._is_back_to_start(start_key_idx, start_model_idx):
                    checked_all = True

        if self.settings.open_router_api_key:
            try:
                return await self._call_openrouter(prompt)
            except OpenRouterPaymentRequired as e:
                logger.error("openrouter_payment_required", error=str(e))
                errors.append(str(e))
            except Exception as e:
                logger.error("openrouter_error", error=str(e))
                errors.append(str(e))

        logger.critical("llm_all_limits_exhausted")
        raise AllLimitsExhausted("; ".join(errors) or "усі ключі та моделі вичерпані")

    def _advance(self) -> None:
        """Переходить до наступної моделі або ключа."""
        self._model_idx += 1
        if self._model_idx >= len(self._models):
            self._model_idx = 0
            self._key_idx += 1
            if self._key_idx >= len(self._keys):
                self._key_idx = 0

    def _is_back_to_start(self, start_key: int, start_model: int) -> bool:
        """Перевіряє, чи повернулися до початкової позиції."""
        return self._key_idx == start_key and self._model_idx == start_model

    async def _throttle(self) -> None:
        async with self._lock:
            delta = time.monotonic() - self._last_call
            wait = self.settings.llm.min_interval_s - delta
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_call = time.monotonic()

    async def _call_gemini_with_wait(self, prompt: str, api_key: str, model: str) -> LLMResponse:
        """Викликає Gemini з очікуванням при rate limit."""
        cfg = self.settings.llm
        url = f"{cfg.gemini_base_url}/models/{model}:generateContent"
        started = time.monotonic()
        wait_start = started

        async with httpx.AsyncClient(timeout=cfg.timeout_s) as client:
            resp = await client.post(
                url,
                params={"key": api_key},
                json={
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {
                        "temperature": cfg.temperature,
                        "maxOutputTokens": cfg.max_tokens,
                    },
                },
            )

        duration_ms = int((time.monotonic() - started) * 1000)

        if resp.status_code == 429:
            if "quota" in resp.text.lower() or "exceeded" in resp.text.lower():
                raise GeminiQuotaExceeded(f"429 quota: {resp.text[:200]}")
            wait_time = time.monotonic() - wait_start
            if wait_time < cfg.daily_limit_wait_s:
                remaining = cfg.daily_limit_wait_s - wait_time
                logger.info("gemini_rate_limited_waiting", wait_s=remaining)
                await asyncio.sleep(remaining)
                return await self._call_gemini_with_wait(prompt, api_key, model)
            raise GeminiRateLimited(f"429: {resp.text[:200]}")
        if resp.status_code in (401, 403):
            raise GeminiAuthError(f"{resp.status_code}: {resp.text[:200]}")
        resp.raise_for_status()

        data = resp.json()
        try:
            text = data["candidates"][0]["content"]["parts"][0]["text"]
        except (KeyError, IndexError, TypeError) as e:
            raise LLMUnavailable(f"несподівана відповідь Gemini: {e}") from e

        logger.debug(
            "llm_gemini_ok", model=model, duration_ms=duration_ms, chars=len(text)
        )
        return LLMResponse(text=text, provider="gemini", model=model, duration_ms=duration_ms)

    async def _call_openrouter(self, prompt: str) -> LLMResponse:
        cfg = self.settings.llm
        started = time.monotonic()

        async with httpx.AsyncClient(timeout=cfg.timeout_s) as client:
            resp = await client.post(
                f"{cfg.openrouter_base_url}/chat/completions",
                headers={"Authorization": f"Bearer {self.settings.open_router_api_key}"},
                json={
                    "model": cfg.openrouter_model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": cfg.temperature,
                    "max_tokens": cfg.max_tokens,
                },
            )

        duration_ms = int((time.monotonic() - started) * 1000)

        if resp.status_code == 429:
            raise LLMUnavailable("openrouter 429")
        if resp.status_code == 402:
            raise OpenRouterPaymentRequired(f"402: {resp.text[:200]}")
        resp.raise_for_status()

        data = resp.json()
        try:
            text = data["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as e:
            raise LLMUnavailable(f"несподівана відповідь OpenRouter: {e}") from e

        logger.debug(
            "llm_openrouter_ok",
            model=cfg.openrouter_model,
            duration_ms=duration_ms,
            chars=len(text),
        )
        return LLMResponse(
            text=text, provider="openrouter", model=cfg.openrouter_model, duration_ms=duration_ms
        )


class GeminiRateLimited(Exception):
    pass


class GeminiQuotaExceeded(Exception):
    pass


class GeminiAuthError(Exception):
    pass


class OpenRouterPaymentRequired(Exception):
    pass
