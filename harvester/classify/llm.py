import asyncio
import time
from dataclasses import dataclass

import httpx
import structlog

from harvester.classify.ratelimit import DailyLimitExhausted, ModelRateLimiter
from harvester.config import get_settings

logger = structlog.get_logger()

# Обмеження повторних критичних логів про вичерпання: стан денний,
# тому дублювання щосекунди лише засмічує лог.
_ALL_EXHAUSTED_LOG_INTERVAL = 300.0  # секунд (5 хв)
_last_all_exhausted_log: float = 0.0


class LLMUnavailable(Exception):
    """Усі LLM-провайдери недоступні."""


class AllLimitsExhausted(Exception):
    """Усі ключі та моделі вичерпали денні ліміти."""


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
                 gemma_only: bool = False, service: str = "LLM"):
        self.service = service
        self.settings = get_settings()
        # Для gemma_only воркери передають власну пару key/model. Для
        # звичайного клієнта `models` означає лише Gemini-моделі, а Gemma
        # завжди береться з окремого списку конфігурації.
        self._models = [] if gemma_only else (models or self.settings.llm.gemini_models)
        self._gemma_models = (
            models if gemma_only else self.settings.llm.gemma_models
        )
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
        """Ініціалізує стан без пробного API-запиту.

        Пробний запит на старті множив навантаження на всі ключі, споживав
        квоту та помилково трактував тимчасову мережеву помилку як вичерпання.
        Реальна доступність перевіряється під час `complete()` з ротацією.
        """
        if self._initialized:
            return

        self._initialized = True
        self._phase = "gemma" if self._gemma_only else "gemini"
        self._key_idx = 0
        self._model_idx = 0
        logger.info(
            "llm_initialized",
            phase=self._phase,
            keys=len(self._keys),
            gemini_models=len(self._models),
            gemma_models=len(self._gemma_models),
            openrouter=bool(self.settings.open_router_api_key),
        )

    async def complete(self, prompt: str) -> LLMResponse:
        if not self.enabled:
            raise LLMUnavailable("LLM вимкнено або немає ключів")

        await self.initialize()
        await self._throttle()

        errors: list[str] = []

        # === Фаза 1: Gemini ===
        if not self._gemma_only and self._keys and self._models:
            gemini_ok = await self._run_phase(
                prompt, self._models, self._daily_limit_exhausted, "gemini", errors
            )
            if gemini_ok is not None:
                return gemini_ok

        # === Фаза 2: Gemma ===
        gemma_prompt = rephrase_for_gemma(prompt, self.settings.llm.gemma_max_chars)
        if self._keys and self._gemma_models:
            gemma_ok = await self._run_phase(
                gemma_prompt, self._gemma_models, self._gemma_limit_exhausted, "gemma", errors
            )
            if gemma_ok is not None:
                return gemma_ok

        # === Фаза 3: OpenRouter ===
        if self.settings.open_router_api_key:
            try:
                return await self._call_openrouter(prompt)
            except Exception as e:  # noqa: BLE001
                errors.append(f"OpenRouter: {e}")
                logger.warning("openrouter_unavailable", error=str(e)[:300])

        combinations = 0
        exhausted_combinations = 0
        if not self._gemma_only:
            combinations += len(self._keys) * len(self._models)
            exhausted_combinations += len(self._daily_limit_exhausted)
        combinations += len(self._keys) * len(self._gemma_models)
        exhausted_combinations += len(self._gemma_limit_exhausted)

        if combinations and exhausted_combinations >= combinations:
            global _last_all_exhausted_log
            if time.monotonic() - _last_all_exhausted_log >= _ALL_EXHAUSTED_LOG_INTERVAL:
                _last_all_exhausted_log = time.monotonic()
                logger.critical("llm_all_limits_exhausted")
                # Сповіщення на пошту про вичерпання всіх LLM
                try:
                    from harvester.core.notify import notify_llm_all_exhausted
                    await notify_llm_all_exhausted(errors, service=self.service)
                except Exception:
                    pass
            raise AllLimitsExhausted("; ".join(errors) or "усі ключі та моделі вичерпані")

        if not errors:
            raise LLMUnavailable("Немає налаштованого доступного LLM-провайдера")
        logger.warning("llm_unavailable", errors=errors)
        raise LLMUnavailable("; ".join(errors))

    async def _run_phase(
        self,
        prompt: str,
        models: list[str],
        exhausted: set[tuple[int, int]],
        phase: str,
        errors: list[str],
    ) -> LLMResponse | None:
        """Запускає цикл ротації моделей×ключів для однієї фази."""
        if not self._keys or not models:
            return None
        self._phase = phase
        self._key_idx = 0
        self._model_idx = 0
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
                )
                # 429 з quota/exceeded уже є достовірним сигналом для цієї
                # пари key/model; додатковий probe лише споживає квоту.
                exhausted.add((self._key_idx, self._model_idx))
                errors.append(f"{model}[key{self._key_idx}]: quota")
                self._advance_phase(models)
                transient_retries = 0
                if self._is_back_to_start(start_key_idx, start_model_idx):
                    checked_all = True
            except GeminiRateLimited as e:
                logger.warning("gemini_rate_limited", phase=phase, key_idx=self._key_idx, model=model)
                errors.append(str(e))
                transient_retries += 1
                if transient_retries < MAX_TRANSIENT_RETRIES:
                    await asyncio.sleep(2 * transient_retries)
                    continue
                self._advance_phase(models)
                transient_retries = 0
                if self._is_back_to_start(start_key_idx, start_model_idx):
                    checked_all = True
            except GeminiAuthError as e:
                logger.error("gemini_auth_error", phase=phase, key_idx=self._key_idx, model=model, error_msg=str(e))
                errors.append(str(e))
                exhausted.add((self._key_idx, self._model_idx))
                try:
                    from harvester.core.notify import notify_llm_failure
                    await notify_llm_failure(phase, model, f"Auth error: {e}", service=self.service)
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
                    await notify_llm_failure(phase, model, f"[{error_type}] {error_msg[:200]}", error_type=error_type, service=self.service)
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
        while True:
            async with self._lock:
                delta = time.monotonic() - self._last_call
                wait = self.settings.llm.min_interval_s - delta
                if wait <= 0:
                    self._last_call = time.monotonic()
                    return
            await asyncio.sleep(wait)

    async def _call_gemini_with_wait(
        self, prompt: str, api_key: str, model: str, phase: str = "gemini"
    ) -> LLMResponse:
        """Викликає Gemini/Gemma з очікуванням при rate limit."""
        cfg = self.settings.llm
        url = f"{cfg.gemini_base_url}/models/{model}:generateContent"
        started = time.monotonic()
        for attempt in range(3):
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

            if resp.status_code == 429:
                if "quota" in resp.text.lower() or "exceeded" in resp.text.lower():
                    raise GeminiQuotaExceeded(f"429 quota: {resp.text[:200]}")
                if attempt < 2:
                    wait_s = min(5 * (attempt + 1), cfg.daily_limit_wait_s)
                    logger.info("gemini_rate_limited_waiting", phase=phase, wait_s=wait_s)
                    await asyncio.sleep(wait_s)
                    continue
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
                if not text:
                    text = parts[-1].get("text", "")
            except (KeyError, IndexError, TypeError) as e:
                raise LLMUnavailable(f"несподівана відповідь Gemini: {e}") from e

            usage = data.get("usageMetadata", {})
            total_tokens = usage.get("totalTokenCount", 0)
            if total_tokens:
                self._rate_limiter.record_tokens(model, total_tokens)

            duration_ms = int((time.monotonic() - started) * 1000)
            log_fn = logger.debug if phase == "gemini" else logger.info
            log_fn(
                f"llm_{phase}_ok",
                model=model,
                duration_ms=duration_ms,
                chars=len(text),
                tokens=total_tokens,
            )
            return LLMResponse(text=text, provider=phase, model=model, duration_ms=duration_ms)

        raise GeminiRateLimited(f"429: перевищено кількість повторів для {model}")

    async def _call_openrouter(self, prompt: str) -> LLMResponse:
        """Виконує останній фолбек через OpenAI-сумісний OpenRouter API."""
        cfg = self.settings.llm
        api_key = self.settings.open_router_api_key
        if not api_key:
            raise LLMUnavailable("OpenRouter ключ не налаштовано")

        started = time.monotonic()
        async with httpx.AsyncClient(timeout=cfg.timeout_s) as client:
            response = await client.post(
                f"{cfg.openrouter_base_url.rstrip('/')}/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://harvester.local",
                    "X-Title": "Harvester",
                },
                json={
                    "model": cfg.openrouter_model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": cfg.temperature,
                    "max_tokens": cfg.max_tokens,
                },
            )

        if response.status_code in (401, 403):
            raise LLMUnavailable(f"OpenRouter {response.status_code}: {response.text[:200]}")
        response.raise_for_status()
        try:
            data = response.json()
            content = data["choices"][0]["message"]["content"]
            if isinstance(content, list):
                content = "".join(
                    part.get("text", "") for part in content if isinstance(part, dict)
                )
            text = str(content or "")
        except (KeyError, IndexError, TypeError, ValueError) as e:
            raise LLMUnavailable(f"несподівана відповідь OpenRouter: {e}") from e

        if not text.strip():
            raise LLMUnavailable("OpenRouter повернув порожню відповідь")
        duration_ms = int((time.monotonic() - started) * 1000)
        logger.info(
            "llm_openrouter_ok",
            model=cfg.openrouter_model,
            duration_ms=duration_ms,
            chars=len(text),
        )
        return LLMResponse(
            text=text,
            provider="openrouter",
            model=cfg.openrouter_model,
            duration_ms=duration_ms,
        )

class GeminiRateLimited(Exception):
    """Модель тимчасово обмежила частоту запитів."""


class GeminiQuotaExceeded(Exception):
    """Для пари key/model вичерпано квоту."""


class GeminiAuthError(Exception):
    """Ключ не авторизований або відкликаний."""


class OpenRouterPaymentRequired(Exception):
    """OpenRouter вимагає оплату або вичерпано баланс."""
