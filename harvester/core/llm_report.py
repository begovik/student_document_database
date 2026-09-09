"""Звіт по використанню LLM-моделей і лімітів з логів Harvester.

Аналізує JSON-лог (`logs/harvester.log*`), агрегує події LLM по днях:
успішні виклики, токени, середній час відповіді, та події лімітів
(quota, rate-limit, тимчасові помилки, повне вичерпання).

Використовується командою `harvester report --llm`.
"""

import json
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

# Події, що вважаються "успішним LLM-викликом"
OK_EVENTS = {"llm_gemma_ok", "verifier_llm_ok"}
# Подія класифікації документа (окремо, бо не є викликом сам по собі)
CLASSIFIED_EVENT = "llm_classified"
# Ліміти
QUOTA_EVENTS = {"gemini_quota_exceeded", "gemini_daily_limit_confirmed"}
RATE_EVENTS = {"rate_limit_tpm", "rate_limit_rpm", "gemini_rate_limited"}
TRANSIENT_EVENTS = {"gemini_transient_error", "gemini_error"}
EXHAUSTED_EVENTS = {"llm_all_limits_exhausted", "classify_worker_all_limits_exhausted"}
UNAVAILABLE_EVENTS = {"llm_unavailable", "llm_unavailable_fallback_rules"}


class LLMDayStats:
    """Статистика LLM за один день."""

    __slots__ = (
        "classified",
        "duration_ms",
        "exhausted",
        "models",
        "ok",
        "quota",
        "rate",
        "tokens",
        "transient",
        "unavailable",
    )

    def __init__(self) -> None:
        self.ok = 0
        self.tokens = 0
        self.duration_ms = 0
        self.classified = 0
        self.quota = 0
        self.rate = 0
        self.transient = 0
        self.exhausted = 0
        self.unavailable = 0
        self.models: dict[str, LLMModelStats] = defaultdict(LLMModelStats)

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "tokens": self.tokens,
            "avg_duration_ms": int(self.duration_ms / self.ok) if self.ok else 0,
            "classified": self.classified,
            "quota": self.quota,
            "rate": self.rate,
            "transient": self.transient,
            "exhausted": self.exhausted,
            "unavailable": self.unavailable,
            "models": {
                name: stats.as_dict() for name, stats in sorted(self.models.items())
            },
        }


class LLMModelStats:
    """Статистика LLM по конкретній моделі."""

    __slots__ = ("duration_ms", "ok", "quota", "rate", "tokens", "transient")

    def __init__(self) -> None:
        self.ok = 0
        self.tokens = 0
        self.duration_ms = 0
        self.quota = 0
        self.rate = 0
        self.transient = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "tokens": self.tokens,
            "avg_duration_ms": int(self.duration_ms / self.ok) if self.ok else 0,
            "quota": self.quota,
            "rate": self.rate,
            "transient": self.transient,
        }


def _event_model(rec: dict[str, Any]) -> str:
    if rec.get("event") == "verifier_llm_ok":
        return "verifier-LLM"
    model = rec.get("model")
    if model:
        return model
    phase = rec.get("phase")
    return phase or "unknown"


def _parse_log_lines(lines: list[str], stats_by_day: dict[str, LLMDayStats],
                     present_days: set[str]) -> None:
    for line in lines:
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            rec = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            continue
        event = rec.get("event")
        ts = rec.get("ts")
        if not event or not ts:
            continue
        day = str(ts)[:10]
        if day not in stats_by_day:
            continue
        present_days.add(day)
        day_stats = stats_by_day[day]
        model = _event_model(rec)
        if event == CLASSIFIED_EVENT:
            day_stats.classified += 1
            continue
        if event in OK_EVENTS:
            day_stats.ok += 1
            tokens = int(rec.get("tokens") or 0)
            duration = int(rec.get("duration_ms") or 0)
            day_stats.tokens += tokens
            day_stats.duration_ms += duration
            day_stats.models[model].ok += 1
            day_stats.models[model].tokens += tokens
            day_stats.models[model].duration_ms += duration
        elif event in QUOTA_EVENTS:
            day_stats.quota += 1
            day_stats.models[model].quota += 1
        elif event in RATE_EVENTS:
            day_stats.rate += 1
            day_stats.models[model].rate += 1
        elif event in TRANSIENT_EVENTS:
            day_stats.transient += 1
            day_stats.models[model].transient += 1
        elif event in EXHAUSTED_EVENTS:
            day_stats.exhausted += 1
        elif event in UNAVAILABLE_EVENTS:
            day_stats.unavailable += 1


def build_llm_report(log_dir: str | Path = "logs", days: int = 7,
                     log_prefix: str = "harvester.log") -> dict[str, Any]:
    """Побудувати звіт LLM за останні N днів.

    Args:
        log_dir: каталог з логами
        days: період у днях
        log_prefix: префікс файлів логу (з урахуванням ротації)

    Returns:
        dict: {"generated_at", "days", "period": {"from","to"}, "by_day": {...}, "by_model": {...}}
    """
    today = datetime.utcnow()
    start = today - timedelta(days=days - 1)
    days_range = [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]

    stats_by_day = {d: LLMDayStats() for d in days_range}
    present_days: set[str] = set()

    log_dir = Path(log_dir)
    files = sorted(log_dir.glob(f"{log_prefix}*"))
    for path in files:
        if path.is_dir():
            continue
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as fh:
                lines = fh.readlines()
        except OSError:
            continue
        _parse_log_lines(lines, stats_by_day, present_days)

    by_day = {d: stats_by_day[d].as_dict() for d in days_range}
    model_totals: dict[str, LLMModelStats] = defaultdict(LLMModelStats)
    for stats in stats_by_day.values():
        for name, ms in stats.models.items():
            totals = model_totals[name]
            totals.ok += ms.ok
            totals.tokens += ms.tokens
            totals.duration_ms += ms.duration_ms
            totals.quota += ms.quota
            totals.rate += ms.rate
            totals.transient += ms.transient

    return {
        "generated_at": today.isoformat(),
        "days": days,
        "period": {"from": days_range[0], "to": days_range[-1]},
        "log_coverage_from": min(present_days) if present_days else None,
        "by_day": by_day,
        "by_model": {name: ms.as_dict() for name, ms in sorted(model_totals.items())},
    }