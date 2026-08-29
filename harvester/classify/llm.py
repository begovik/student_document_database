import asyncio
import time
from dataclasses import dataclass

import httpx
import structlog

from harvester.classify.ratelimit import ModelRateLimiter, DailyLimitExhausted
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


def rephrase_for_gemma(text: str, max_chars: int = 15000) -> str:
    """Стискає текст для Gemma (16k контекст) зі збереженням сенсу.

    Алгоритм:
    1. Якщо текст вже коротший за max_chars — повертає як є.
    2. Шукає межі речень (; . ! ?) щоб обрізати на логічній межі.
    3. Зберігає початок (перші 60%) та кінець (останні 30%) тексту,
       пропускаючи середину — так зберігаються і вступ, і висновки.
    4. Додає позначку про стиснення.
    """
    if len(text) <= max_chars:
        return text

    reserve = 100
    budget = max_chars - reserve

    sentences = []
    current: list[str] = []
    for char in text:
        current.append(char)
        if char in ".!?\n" or (char == ";" and len(current) > 20):
            sentences.append("".join(current))
            current = []
    if current:
        sentences.append("".join(current))

    if len(sentences) <= 3:
        return text[:max_chars] + "\n[обрізано]"

    head_budget = int(budget * 0.6)
    tail_budget = int(budget * 0.3)

    head_parts: list[str] = []
    head_len = 0
    for s in sentences:
        if head_len + len(s) > head_budget:
            break
        head_parts.append(s)
        head_len += len(s)

    tail_parts: list[str] = []
    tail_len = 0
    for s in reversed(sentences):
        if tail_len + len(s) > tail_budget:
            break
        tail_parts.append(s)
        tail_len += len(s)
    tail_parts.reverse()

    skipped = len(sentences) - len(head_parts) - len(tail_parts)
    head_text = "".join(head_parts)
    tail_text = "".join(tail_parts)

    result = f"{head_text}\n[пропущено {skipped} речень з {len(sentences)} — стиснуто для Gemma]\n{tail_text}"
    return result[:max_chars]


class LLMClient:
    """
    Двофазний LLM-клієнт з ротацією моделей та ключів.

    Фаза 1 — Gemini (gemini-3.1-flash-lite, gemini-3.5-flash-lite) × 3 ключі:
      - контекст 250k, але обмежені денні ліміти
    Фаза 2 — Gemma 4 (gemma-4-31b-it, gemini-4-26b-it) × 3 ключі:
      - величезні денні ліміти, але контекст 16k → текст перефразовується
    Фолбек — OpenRouter (google/gemini-2.5-flash)
    """

    def __init__(self, keys: list[str] | None = None, models: list[str] | None = None,
                 gemma_only: bool = False):
        self.settings = get_settings()
        self._models = [] if gemma_only else (models or self.settings.llm.gemini_models)
        self._gemma_models = models or self.settings.llm.gemma_models
        self._keys = keys or self.settings.gemini_keys
        self._gemma_only = gemma_only
        self._model_idx = 0
        self._key_idx = 0
        self._last_call = 0.0
        self._lock = asyncio.Lock()
        self._daily_limit_exhausted: set[tuple[int, int]] = set()
        self._gemma_limit_exhausted: set[tuple[int, int]] = set()
        self._phase = "gemma" if gemma_only else "gemini"
        self._initialized = False
        self._rate_limiter = ModelRateLimiter(
            gemini_rpm=self.settings.llm.gemini_rpm,
            gemini_rpd=self.settings.llm.gemini_rpd,
            gemma_rpm=self.settings.llm.gemma_rpm,
            gemma_rpd=self.settings.llm.gemma_rpd,
            gemma_tpm=self.settings.llm.gemma_tpm,
        )

    @property
    def enabled(self) -> bool:
        return self.settings.llm.enabled and bool(
            self._keys or self.settings.open_router_api_key
        )

    async def initialize(self) -> None:
        """Перевіряє моделі при запуску, пропускає вичерпані."""
        if self._initialized:
            return

        self._initialized = True

        if not self._keys:
            return

        # Фаза 1: Gemini
        for key_idx in range(len(self._keys)):
            for model_idx in range(len(self._models)):
                key = self._keys[(self._key_idx + key_idx) % len(self._keys)]
                model = self._models[(self._model_idx + model_idx) % len(self._models)]

                available = await self._check_model_available(key, model)
                if available:
                    self._key_idx = (self._key_idx + key_idx) % len(self._keys)
                    self._model_idx = (self._model_idx + model_idx) % len(self._models)
                    self._phase = "gemini"
                    logger.info(
                        "llm_initialized",
                        phase="gemini",
                        key_idx=self._key_idx,
                        model=self._models[self._model_idx],
                    )
                    return
                else:
                    logger.warning(
                        "llm_model_exhausted_at_startup",
                        phase="gemini",
                        key_idx=(self._key_idx + key_idx) % len(self._keys),
                        model=model,
                    )

        # Фаза 2: Gemma — Gemini вичерпані, пробуємо Gemma
        logger.info("gemini_all_exhausted_at_startup", phase="gemma")
        self._phase = "gemma"
        self._key_idx = 0
        self._model_idx = 0

        for key_idx in range(len(self._keys)):
            for model_idx in range(len(self._gemma_models)):
                key = self._keys[(self._key_idx + key_idx) % len(self._keys)]
                model = self._gemma_models[(self._model_idx + model_idx) % len(self._gemma_models)]

                available = await self._check_model_available(key, model)
                if available:
                    self._key_idx = (self._key_idx + key_idx) % len(self._keys)
                    self._model_idx = (self._model_idx + model_idx) % len(self._gemma_models)
                    logger.info(
                        "llm_initialized",
                        phase="gemma",
                        key_idx=self._key_idx,
                        model=self._gemma_models[self._model_idx],
                    )
                    return
                else:
                    logger.warning(
                        "llm_model_exhausted_at_startup",
                        phase="gemma",
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
            raise LLMUnavailable("Немає Gemma ключів")

        errors: list[str] = []

        # === Фаза 2: Gemma (gemma_models × keys) ===
        gemma_prompt = rephrase_for_gemma(prompt, self.settings.llm.gemma_max_chars)
        gemma_ok = await self._run_phase(
            gemma_prompt, self._gemma_models, self._gemma_limit_exhausted, "gemma", errors
        )
        if gemma_ok is not None:
            return gemma_ok

        # Розрізняємо справжнє вичерпання лімітів та тимчасові 5xx
        has_daily_limit = any("daily limit" in e.lower() for e in errors)
        has_transient = any(
            any(code in e for code in ["500", "502", "503", "504", "timeout", "ReadError", "ConnectError", "RemoteProtocolError"])
            for e in errors
        )
        # Якщо немає daily limit, а є лише transient 5xx/timeout — це не AllLimitsExhausted
        if has_transient and not has_daily_limit:
            logger.warning("llm_transient_unavailable", errors=errors)
            raise LLMUnavailable("; ".join(errors) or "тимчасова недоступність LLM (5xx/timeout)")

        logger.critical("llm_all_limits_exhausted")
        # Сповіщення на пошту про вичерпання всіх LLM
        try:
            from harvester.core.notify import notify_llm_all_exhausted
            await notify_llm_all_exhausted(errors)
        except Exception:
            pass
        raise AllLimitsExhausted("; ".join(errors) or "усі ключі та моделі вичерпані")

    async def _run_phase(
        self,
        prompt: str,
        models: list[str],
        exhausted: set[tuple[int, int]],
        phase: str,
        errors: list[str],
    ) -> LLMResponse | None:
        """Запускає цикл ротації моделей×ключів для однієї фази."""
        start_key_idx = self._key_idx
        start_model_idx = self._model_idx
        checked_all = False
        # Лічильник тимчасових помилок для поточної комбінації (key,model)
        MAX_TRANSIENT_RETRIES = 3
        transient_retries: int = 0
        transient_backoff = [3, 6, 12]

        while not checked_all:
            key = self._keys[self._key_idx]
            model = models[self._model_idx]

            if (self._key_idx, self._model_idx) in exhausted:
                self._advance_phase(models)
                if self._is_back_to_start(start_key_idx, start_model_idx):
                    checked_all = True
                continue

            try:
                result = await self._call_gemini_with_wait(prompt, key, model, phase)
                return result
            except DailyLimitExhausted:
                logger.warning("daily_limit_exhausted", phase=phase, key_idx=self._key_idx, model=model)
                exhausted.add((self._key_idx, self._model_idx))
                errors.append(f"{model}[key{self._key_idx}]: daily limit (rate limiter)")
                self._advance_phase(models)
                transient_retries = 0
                if self._is_back_to_start(start_key_idx, start_model_idx):
                    checked_all = True
            except GeminiQuotaExceeded:
                logger.warning(
                    "gemini_quota_exceeded",
                    phase=phase,
                    key_idx=self._key_idx,
                    model=model,
                    waiting_s=self.settings.llm.daily_limit_wait_s,
                )
                await asyncio.sleep(self.settings.llm.daily_limit_wait_s)

                available = await self._check_model_available(key, model)
                if not available:
                    logger.warning(
                        "gemini_daily_limit_confirmed",
                        phase=phase,
                        key_idx=self._key_idx,
                        model=model,
                    )
                    exhausted.add((self._key_idx, self._model_idx))
                    errors.append(f"{model}[key{self._key_idx}]: daily limit")
                    self._advance_phase(models)
                    transient_retries = 0
                    if self._is_back_to_start(start_key_idx, start_model_idx):
                        checked_all = True
                else:
                    logger.info("gemini_quota_recovered", phase=phase, key_idx=self._key_idx, model=model)
            except GeminiRateLimited as e:
                logger.warning("gemini_rate_limited", phase=phase, key_idx=self._key_idx, model=model)
                errors.append(str(e))
                await asyncio.sleep(2)
            except GeminiAuthError as e:
                logger.error("gemini_auth_error", phase=phase, key_idx=self._key_idx, model=model, error_msg=str(e))
                errors.append(str(e))
                exhausted.add((self._key_idx, self._model_idx))
                try:
                    from harvester.core.notify import notify_llm_failure
                    await notify_llm_failure("gemma", model, f"Auth error: {e}")
                except Exception:
                    pass
                self._advance_phase(models)
                transient_retries = 0
                if self._is_back_to_start(start_key_idx, start_model_idx):
                    checked_all = True
            except Exception as e:
                error_type = type(e).__name__
                error_msg = str(e) or f"[{error_type}] без повідомлення"
                # Витягуємо HTTP status code з повідомлення
                status_code = ""
                for code in ["500", "502", "503", "504", "503"]:
                    if f"'{code}'" in error_msg or f'"code": {code}' in error_msg:
                        status_code = code
                        break

                is_transient = (status_code in ("500", "502", "503", "504") or
                               "timeout" in error_msg.lower() or
                               "ReadError" in error_type or
                               "ConnectError" in error_type or
                               "RemoteProtocolError" in error_type or
                               "HTTPStatusError" in error_type or
                               "PoolTimeout" in error_type)

                if is_transient and transient_retries < MAX_TRANSIENT_RETRIES:
                    wait = transient_backoff[min(transient_retries, len(transient_backoff) - 1)]
                    transient_retries += 1
                    logger.warning("gemini_transient_error", phase=phase, key_idx=self._key_idx,
                                   model=model, error_msg=error_msg[:150], attempt=transient_retries,
                                   wait_s=wait)
                    await asyncio.sleep(wait)
                    continue  # Повторюємо той самий запит

                logger.error("gemini_error", phase=phase, key_idx=self._key_idx, model=model,
                            error_msg=error_msg, error_type=error_type)
                errors.append(error_msg)
                # Критична помилка — відправити на пошту
                try:
                    from harvester.core.notify import notify_llm_failure
                    await notify_llm_failure("gemma", model, f"[{error_type}] {error_msg[:200]}", error_type=error_type)
                except Exception:
                    pass
                self._advance_phase(models)
                transient_retries = 0
                if self._is_back_to_start(start_key_idx, start_model_idx):
                    checked_all = True

        return None

    def _advance_phase(self, models: list[str]) -> None:
        """Переходить до наступної моделі або ключа в межах фази."""
        self._model_idx += 1
        if self._model_idx >= len(models):
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

    async def _call_gemini_with_wait(self, prompt: str, api_key: str, model: str, phase: str = "gemini") -> LLMResponse:
        """Викликає Gemini/Gemma з очікуванням при rate limit."""
        cfg = self.settings.llm
        url = f"{cfg.gemini_base_url}/models/{model}:generateContent"
        started = time.monotonic()
        wait_start = started

        # Per-model rate limiting (RPM, RPD, TPM)
        await self._rate_limiter.acquire(model, phase)

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
                logger.info("gemini_rate_limited_waiting", phase=phase, wait_s=remaining)
                await asyncio.sleep(remaining)
                return await self._call_gemini_with_wait(prompt, api_key, model, phase)
            raise GeminiRateLimited(f"429: {resp.text[:200]}")
        if resp.status_code in (401, 403):
            raise GeminiAuthError(f"{resp.status_code}: {resp.text[:200]}")
        resp.raise_for_status()

        data = resp.json()
        try:
            parts = data["candidates"][0]["content"]["parts"]
            # Шукаємо частину без thought=True (фактична відповідь, а не роздуми)
            text = ""
            for part in parts:
                if not part.get("thought", False):
                    text = part.get("text", "")
                    break
            # Якщо не знайшли — беремо останню частину
            if not text:
                text = parts[-1].get("text", "")
        except (KeyError, IndexError, TypeError) as e:
            raise LLMUnavailable(f"несподівана відповідь Gemini: {e}") from e

        # Підрахунок токенів з usageMetadata
        usage = data.get("usageMetadata", {})
        total_tokens = usage.get("totalTokenCount", 0)
        if total_tokens:
            self._rate_limiter.record_tokens(model, total_tokens)

        log_fn = logger.debug if phase == "gemini" else logger.info
        log_fn(
            f"llm_{phase}_ok",
            model=model,
            duration_ms=duration_ms,
            chars=len(text),
            tokens=total_tokens,
        )
        return LLMResponse(text=text, provider=phase, model=model, duration_ms=duration_ms)

class GeminiRateLimited(Exception):
    pass


class GeminiQuotaExceeded(Exception):
    pass


class GeminiAuthError(Exception):
    pass


class OpenRouterPaymentRequired(Exception):
    pass
