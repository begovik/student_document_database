"""Per-key rate limiter для LLM API з обліком токенів."""

import asyncio
import time
from collections import defaultdict
from datetime import UTC, datetime
from typing import ClassVar

import structlog

logger = structlog.get_logger()


class DailyLimitExhausted(Exception):
    """Денний ліміт для пари (ключ, модель) вичерпано."""


class ModelRateLimiter:
    """Rate limiter з лімітами per-key: RPM, RPD, TPM.

    Квоти Google діють на пару (ключ, модель), тому й вікна ведуться за
    парою. Раніше вікна були спільні на модель, а ліміт множився на кількість
    ключів: один ключ перевищував свою TPM-межу в кілька разів, поки сумарний
    лічильник виглядав спокійно. Наслідком був 429, який код трактував як
    вичерпання денної квоти й «забанював» пару до опівночі.

    Токени бронюються ДО запиту і уточнюються фактичним `totalTokenCount`
    після відповіді. Без попередньої броні паралельні запити не бачили один
    одного у вікні й сумарно перевищували ліміт (виміряно 28.5K токенів за
    хвилину при ліміті 16K на ключ).
    """

    _shared_instances: ClassVar[dict[tuple, "ModelRateLimiter"]] = {}

    # TPM-вікно тепер спрацьовує штатно й часто, тому повторювані
    # повідомлення про очікування не мають засмічувати журнал.
    _LOG_THROTTLE_S: ClassVar[float] = 60.0

    def __init__(
        self,
        gemini_rpm: int = 15,
        gemini_rpd: int = 500,
        gemma_rpm: int = 50,
        gemma_rpd: int = 14000,
        gemma_tpm: int = 16000,
        gemini_tpm: int = 250000,
    ):
        self._limits: dict[str, dict[str, int]] = {
            "gemini": {"rpm": gemini_rpm, "rpd": gemini_rpd, "tpm": gemini_tpm},
            "gemma": {"rpm": gemma_rpm, "rpd": gemma_rpd, "tpm": gemma_tpm},
        }
        # Вікна за парою (ключ, модель): саме на неї діють квоти Google.
        self._requests: dict[tuple[str, str], list[float]] = defaultdict(list)
        # Записи токенів: [час, токени, id броні]. Бронь id=0 означає
        # «додано без бронювання» (факт без попередньої оцінки).
        self._tokens: dict[tuple[str, str], list[list]] = defaultdict(list)
        self._daily_counts: dict[tuple[str, str], int] = defaultdict(int)
        self._daily_date: str = datetime.now(UTC).date().isoformat()
        self._reservation_seq = 0
        # Розміри пулів ключів фаз — лише для діагностики. Раніше тут
        # масштабували ліміт, через що сумарний лічильник дозволяв
        # перевищення на кожному окремому ключі.
        self._phase_keys: dict[str, int] = {}
        self._last_log: dict[tuple[str, str], float] = {}
        # Спільне на весь процес знання про вичерпані денні квоти й про
        # накопичені тротлінги. Колись це було в кожному LLM-клієнті окремо,
        # тож кожен воркер з'ясовував те саме заново: 32 воркери × 4 ключі
        # давали сотні марних 429-запитів перед кожним документом.
        self._daily_exhausted: set[tuple[str, str]] = set()
        self._parked: dict[tuple[str, str], float] = {}
        self._lock = asyncio.Lock()

    def register_phase_keys(self, phase: str, keys_count: int) -> None:
        """Запам'ятати розмір пулу ключів фази (тільки для діагностики).

        Квота належить ключу, тому множення ліміту на кількість ключів
        прибрано: воно дозволяло перевищити межу на кожному окремому ключі,
        поки спільний лічильник лишався в нормі.
        """
        self._phase_keys[phase] = max(1, int(keys_count or 1))

    @classmethod
    def shared(
        cls,
        gemini_rpm: int = 15,
        gemini_rpd: int = 500,
        gemma_rpm: int = 50,
        gemma_rpd: int = 14000,
        gemma_tpm: int = 16000,
        gemini_tpm: int = 250000,
    ) -> "ModelRateLimiter":
        """Повернути один limiter на процес для однакового набору квот.

        Один ключ може обслуговувати кілька worker-ів. Окремі екземпляри
        limiter-а в кожному worker-і дозволяли сумарно перевищити RPD/RPM.
        """
        key = (gemini_rpm, gemini_rpd, gemma_rpm, gemma_rpd, gemma_tpm, gemini_tpm)
        instance = cls._shared_instances.get(key)
        if instance is None:
            instance = cls(*key)
            cls._shared_instances[key] = instance
        return instance

    @classmethod
    def reset_shared(cls) -> None:
        """Очистити registry (використовується тестами та повторною ініціалізацією)."""
        cls._shared_instances.clear()

    async def acquire(
        self, model: str, phase: str, key_ref: str = "", estimated_tokens: int = 0
    ) -> int:
        """Забронювати слот запиту; повертає id броні токенів (0 — без броні).

        estimated_tokens — оцінка «промпт + бюджет виводу» ДО запиту. Вона
        тримає місце у TPM-вікні, поки запит у польоті, і замінюється
        фактичним totalTokenCount після відповіді.
        """
        limits = self._limits.get(phase)
        if not limits:
            return 0
        rpm_limit = int(limits.get("rpm") or 0)
        rpd_limit = int(limits.get("rpd") or 0)
        tpm_limit = int(limits.get("tpm") or 0)
        window = (key_ref, model)

        while True:
            wait_s = 0.0
            async with self._lock:
                self._cleanup(window)
                self._check_daily_reset()

                # RPM — запитів за хвилину (для конкретного ключа).
                if rpm_limit and len(self._requests[window]) >= rpm_limit:
                    wait_s = max(self._requests[window][0] + 60 - time.monotonic(), 0.0)
                    if wait_s:
                        self._log_limit(
                            "rate_limit_rpm", window, model=model, phase=phase,
                            used=len(self._requests[window]), limit=rpm_limit,
                            wait_s=round(wait_s, 1), key=key_ref,
                        )

                # RPD — запитів за день (для конкретного ключа).
                if not wait_s and rpd_limit and self._daily_counts[window] >= rpd_limit:
                    self._log_limit(
                        "rate_limit_rpd", window, model=model, phase=phase,
                        used=self._daily_counts[window], limit=rpd_limit, key=key_ref,
                    )
                    raise DailyLimitExhausted(
                        f"{model}: денний ліміт {rpd_limit} вичерпано "
                        f"(ключ {key_ref or '-'})"
                    )

                # TPM — токенів за хвилину (для конкретного ключа).
                if not wait_s and tpm_limit and estimated_tokens:
                    total = sum(entry[1] for entry in self._tokens[window])
                    if total + estimated_tokens > tpm_limit and self._tokens[window]:
                        # Порожнє вікно не чекає: інакше запит, більший за
                        # ліміт, чекав би вічно.
                        wait_s = max(self._tokens[window][0][0] + 60 - time.monotonic(), 0.0)
                        if wait_s:
                            self._log_limit(
                                "rate_limit_tpm", window, model=model, phase=phase,
                                used=total, reserve=estimated_tokens, limit=tpm_limit,
                                wait_s=round(wait_s, 1), key=key_ref,
                            )

                if not wait_s:
                    self._requests[window].append(time.monotonic())
                    self._daily_counts[window] += 1
                    if estimated_tokens > 0:
                        self._reservation_seq += 1
                        reservation = self._reservation_seq
                        self._tokens[window].append(
                            [time.monotonic(), int(estimated_tokens), reservation]
                        )
                        return reservation
                    return 0

            # Сон поза lock: інші ключі та моделі можуть перевіряти свої
            # ліміти, поки ця пара чекає на нове вікно.
            await asyncio.sleep(wait_s)

    def record_tokens(
        self, model: str, key_ref: str, token_count: int, reservation: int = 0
    ) -> None:
        """Уточнити заброньовані токени фактичними.

        Якщо броні не було, запис додається: інакше навантаження зникло б із
        TPM-вікна і наступний запит не побачив би витрати попереднього.
        """
        window = (key_ref, model)
        if reservation:
            for entry in self._tokens[window]:
                if entry[2] == reservation:
                    if token_count and token_count > 0:
                        entry[1] = int(token_count)
                    # Немає фактичної витрати — лишаємо оцінку до кінця вікна:
                    # краще зайва обережність, ніж невидиме навантаження.
                    return
        if token_count and token_count > 0:
            self._tokens[window].append([time.monotonic(), int(token_count), 0])

    def park_pair(self, key_ref: str, model: str, seconds: float = 600.0) -> None:
        """Тимчасово припаркувати пару після неоднозначного тротлінгу.

        Тіло 429 однакове для денної квоти й для TPM, тож відрізнити їх
        неможливо. Замість бану до опівночі пара паркується на кілька
        хвилин: якщо квота справді вичерпана, наступна спроба знову
        отримає 429 і пару перепаркують (ціна — 1 марний запит на
        10 хв); якщо це був хвилинний тротлінг — робота продовжиться
        сама, без ручного втручання. Стан, що самозаліковується,
        принципово не може спричинити вічне блокування.
        """
        self._parked[(key_ref, model)] = time.monotonic() + max(seconds, 1.0)
        if len(self._parked) > 2000:
            now = time.monotonic()
            for stale in [k for k, v in self._parked.items() if v < now]:
                del self._parked[stale]

    def is_parked(self, key_ref: str, model: str) -> bool:
        """Чи припаркована зараз пара (паркування ще не спливло)."""
        until = self._parked.get((key_ref, model))
        if until is None:
            return False
        if until <= time.monotonic():
            del self._parked[(key_ref, model)]
            return False
        return True

    def reset_shared_exhaustion(self) -> None:
        """Очистити спільні позначки вичерпання й паркування.

        Потрібно на початку нового добового вікна або після ручного
        скидання стану клієнта.
        """
        self._daily_exhausted.clear()
        self._parked.clear()

    def mark_daily_exhausted(self, key_ref: str, model: str) -> None:
        """Позначити пару (ключ, модель) як таку, що вичерпала денну квоту."""
        self._daily_exhausted.add((key_ref, model))

    def is_daily_exhausted(self, key_ref: str, model: str) -> bool:
        """Чи відома вже вичерпана денна квота для цієї пари."""
        return (key_ref, model) in self._daily_exhausted

    def snapshot(self, limit: int = 8) -> list[dict]:
        """Стан вікон, у яких була активність — для діагностичного INFO-логу."""
        now = time.monotonic()
        out: list[dict] = []
        for window, tokens in self._tokens.items():
            recent = [(t, tk) for t, tk, _ in tokens if t > now - 60]
            if not recent:
                continue
            key_ref, model = window
            out.append({
                "model": model,
                "key": key_ref,
                "tpm": sum(tk for _, tk in recent),
                "rpd": self._daily_counts.get(window, 0),
            })
        out.sort(key=lambda item: item["tpm"], reverse=True)
        return out[:limit]

    def _log_limit(self, event: str, window: tuple[str, str], **extra) -> None:
        """Записати подію про межу не частіше ніж раз на _LOG_THROTTLE_S на пару."""
        now = time.monotonic()
        last = self._last_log.get((window[0], event), 0.0)
        if now - last < self._LOG_THROTTLE_S:
            return
        self._last_log[(window[0], event)] = now
        if len(self._last_log) > 2000:
            cutoff = now - self._LOG_THROTTLE_S
            for stale in [k for k, v in self._last_log.items() if v < cutoff]:
                del self._last_log[stale]
        logger.warning(event, **extra)

    def _cleanup(self, window: tuple[str, str]) -> None:
        """Видаляє застарілі записи з sliding window (60 сек)."""
        cutoff = time.monotonic() - 60
        self._requests[window] = [t for t in self._requests[window] if t > cutoff]
        self._tokens[window] = [e for e in self._tokens[window] if e[0] > cutoff]

    def _check_daily_reset(self) -> None:
        """Скидає денні лічильники при зміні дати."""
        today = datetime.now(UTC).date().isoformat()
        if self._daily_date != today:
            self._daily_counts.clear()
            self._daily_date = today
            logger.info("rate_limit_daily_reset", date=today)

    def get_stats(self, model: str, phase: str, key_ref: str = "") -> dict:
        """Повертає поточну статистику для пари (ключ, модель)."""
        limits = self._limits.get(phase, {})
        now = time.monotonic()
        window = (key_ref, model)
        rpm = sum(1 for t in self._requests[window] if t > now - 60)
        tpm = sum(entry[1] for entry in self._tokens[window] if entry[0] > now - 60)
        return {
            "model": model,
            "key": key_ref,
            "rpm": rpm,
            "rpm_limit": int(limits.get("rpm") or 0),
            "rpd": self._daily_counts[window],
            "rpd_limit": int(limits.get("rpd") or 0),
            "tpm": tpm,
            "tpm_limit": int(limits.get("tpm") or 0),
        }
