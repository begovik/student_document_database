"""Per-model rate limiter для LLM API з підрахунком токенів."""

import asyncio
import time
from collections import defaultdict
from datetime import date

import structlog

logger = structlog.get_logger()


class DailyLimitExhausted(Exception):
    """Денний ліміт для моделі вичерпано."""
    pass


class ModelRateLimiter:
    """Rate limiter з лімітами per-model: RPM, RPD, TPM (tokens per minute).

    Ліміти:
      Gemini: 15 запитів/хв, 500 запитів/день
      Gemma:  30 запитів/хв, 14000 запитів/день, 16k токенів/хв
    """

    def __init__(
        self,
        gemini_rpm: int = 15,
        gemini_rpd: int = 500,
        gemma_rpm: int = 30,
        gemma_rpd: int = 14000,
        gemma_tpm: int = 16000,
    ):
        self._limits: dict[str, dict[str, int]] = {
            "gemini": {"rpm": gemini_rpm, "rpd": gemini_rpd},
            "gemma": {"rpm": gemma_rpm, "rpd": gemma_rpd, "tpm": gemma_tpm},
        }
        # Sliding window: timestamps запитів per model
        self._requests: dict[str, list[float]] = defaultdict(list)
        # Sliding window: (timestamp, token_count) per model
        self._tokens: dict[str, list[tuple[float, int]]] = defaultdict(list)
        # Daily counters per model
        self._daily_counts: dict[str, int] = defaultdict(int)
        self._daily_date: str = date.today().isoformat()
        self._lock = asyncio.Lock()

    async def acquire(self, model: str, phase: str) -> None:
        """Чекає дозволу на запит до моделі з урахуванням усіх лімітів."""
        limits = self._limits.get(phase)
        if not limits:
            return

        while True:
            async with self._lock:
                self._cleanup(model)
                self._check_daily_reset()

                # RPM — запитів за хвилину
                rpm_limit = limits.get("rpm", 0)
                if rpm_limit and len(self._requests[model]) >= rpm_limit:
                    wait = self._requests[model][0] + 60 - time.monotonic()
                    if wait > 0:
                        logger.warning(
                            "rate_limit_rpm",
                            model=model,
                            rpm=len(self._requests[model]),
                            limit=rpm_limit,
                            wait_s=round(wait, 1),
                        )
                        # Звільняємо lock перед сном
                        await asyncio.sleep(wait)
                        continue

                # RPD — запитів за день
                rpd_limit = limits.get("rpd", 0)
                if rpd_limit and self._daily_counts[model] >= rpd_limit:
                    logger.warning(
                        "rate_limit_rpd",
                        model=model,
                        rpd=self._daily_counts[model],
                        limit=rpd_limit,
                    )
                    # Кидаємо виключення щоб LLM міг перейти до іншої моделі
                    raise DailyLimitExhausted(f"{model}: денний ліміт {rpd_limit} вичерпано")

                # TPM — токенів за хвилину (тільки Gemma)
                tpm_limit = limits.get("tpm", 0)
                if tpm_limit:
                    total_tokens = sum(tk for _, tk in self._tokens[model])
                    if total_tokens >= tpm_limit:
                        wait = self._tokens[model][0][0] + 60 - time.monotonic()
                        if wait > 0:
                            logger.warning(
                                "rate_limit_tpm",
                                model=model,
                                tokens=total_tokens,
                                limit=tpm_limit,
                                wait_s=round(wait, 1),
                            )
                            await asyncio.sleep(wait)
                            continue

                # Все ок — реєструємо запит
                self._requests[model].append(time.monotonic())
                self._daily_counts[model] += 1
                return

    def record_tokens(self, model: str, token_count: int) -> None:
        """Записує використані токени для моделі (для TPM ліміту)."""
        if token_count > 0:
            self._tokens[model].append((time.monotonic(), token_count))

    def _cleanup(self, model: str) -> None:
        """Видаляє застарілі записи з sliding window (60 сек)."""
        now = time.monotonic()
        cutoff = now - 60
        self._requests[model] = [t for t in self._requests[model] if t > cutoff]
        self._tokens[model] = [(t, tk) for t, tk in self._tokens[model] if t > cutoff]

    def _check_daily_reset(self) -> None:
        """Скидає денні лічильники при зміні дати."""
        today = date.today().isoformat()
        if self._daily_date != today:
            self._daily_counts.clear()
            self._daily_date = today
            logger.info("rate_limit_daily_reset", date=today)

    def get_stats(self, model: str, phase: str) -> dict:
        """Повердає поточну статистику для моделі."""
        limits = self._limits.get(phase, {})
        now = time.monotonic()
        rpm = sum(1 for t in self._requests[model] if t > now - 60)
        tpm = sum(tk for t, tk in self._tokens[model] if t > now - 60)
        return {
            "model": model,
            "rpm": rpm,
            "rpm_limit": limits.get("rpm", 0),
            "rpd": self._daily_counts[model],
            "rpd_limit": limits.get("rpd", 0),
            "tpm": tpm,
            "tpm_limit": limits.get("tpm", 0),
        }
