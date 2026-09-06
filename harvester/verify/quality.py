"""Аналіз якості документів: жорсткі фільтри + оцінка 0..100.

Використовується для відбору лише повноцінних джерел (монографії, підручники,
посібники, статті з повним текстом) перед побудовою каталогу. Виключає
фрагменти, тези, змісти, анотації та журнальні обкладинки.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from harvester.config import FilterRules, get_filter_rules

# Мінімальні вимоги для «цілісного джерела» (жорсткі фільтри)
MIN_PAGE_COUNT = 4
MIN_TEXT_CHARS = 500

# Тип документа → вага/бонус при оцінці якості (науковість джерела)
TYPE_BONUS: dict[str, float] = {
    "book": 25.0,          # монографія/книга — найповніше джерело
    "textbook": 22.0,      # підручник
    "methodical": 20.0,    # метод. вказівки
    "dissertation": 20.0,  # дисертація
    "thesis": 15.0,        # кваліфікаційна робота
    "article": 12.0,       # наукова стаття
    "report": 8.0,         # звіт
    "preprint": 8.0,       # препринт
    "other": 5.0,          # невизначений — найменший бонус
}

# Типи, які НЕ є повноцінним джерелом (журнальні обкладинки, тези тощо)
FRAGMENT_TYPES = {"other"}


@dataclass
class QualityResult:
    """Результат аналізу якості одного документа."""

    passed: bool
    score: float = 0.0
    hard_failures: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)


def _as_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _text_len(doc: dict[str, Any]) -> int:
    """Загальна довжина доступного тексту документа (best-effort).

    Джерела: text_length, text_sample, extra.body, quotations, summary.
    Якщо жодного текстового поля немає — повертає 0 (невідомо).
    """
    text_length = _as_int(doc.get("text_length"))
    if text_length:
        return text_length

    parts: list[str] = []
    text_sample = doc.get("text_sample")
    if text_sample:
        parts.append(str(text_sample))
    extra = parse_extra(doc)
    body = extra.get("body")
    if body:
        parts.append(str(body))
    quotations = doc.get("quotations")
    if isinstance(quotations, list):
        for q in quotations:
            if isinstance(q, dict) and q.get("text"):
                parts.append(str(q["text"]))
            elif isinstance(q, str):
                parts.append(q)
    summary = doc.get("summary")
    if isinstance(summary, dict) and summary.get("text"):
        parts.append(str(summary["text"]))

    return sum(len(p) for p in parts)


class DocumentQualityAnalyzer:
    """Аналізує документи на відповідність цільовим джерелам.

    Жорсткі фільтри (hard filters):
    - page_count >= 4 (повноцінний текст, а не фрагмент/обкладинка)
    - total_text_chars >= 500
    - has_text_layer == 1

    Оцінка 0..100 враховує тип документа, кількість сторінок, наявність
    авторів/УДК/року та наявність текстового шару.
    """

    def __init__(self, rules: FilterRules | None = None):
        self.rules = rules or get_filter_rules("strict")
        self.min_page_count = max(MIN_PAGE_COUNT, _as_int(self.rules.min_page_count, MIN_PAGE_COUNT))
        # мінімальна загальна довжина тексту (фіксована константа)
        self.min_text_chars = MIN_TEXT_CHARS

    def analyze(self, doc: dict[str, Any]) -> QualityResult:
        """Розрахувати якість документа. Не змінює БД."""
        failures: list[str] = []
        details: dict[str, Any] = {
            "page_count": _as_int(doc.get("page_count"), 0),
            "has_text_layer": _as_int(doc.get("has_text_layer"), 0),
            "text_chars": _text_len(doc),
            "doc_type": doc.get("doc_type") or "other",
            "language": doc.get("language"),
        }

        # --- Жорсткі фільтри ---
        page_count = details["page_count"]
        if page_count < self.min_page_count:
            failures.append(f"page_count={page_count} < {self.min_page_count}")

        text_chars = details["text_chars"]
        if text_chars and text_chars < self.min_text_chars:
            failures.append(f"text_chars={text_chars} < {self.min_text_chars}")

        if details["has_text_layer"] != 1:
            failures.append(f"has_text_layer={details['has_text_layer']}")

        # Руський/радянський фільтр (продовжуємо політику проєкту)
        language = (details.get("language") or "").lower()
        if language == "ru":
            failures.append("russian_language")

        # Презентації PowerPoint — не є повноцінним джерелом
        if self.rules.reject_ppt:
            producer = parse_extra(doc).get("producer") or ""
            title_lower = str(doc.get("title") or "").lower()
            if (
                "powerpoint" in str(producer).lower()
                or "ppt" in str(producer).lower()
                or "презентація" in title_lower
                or "presentation" in title_lower
                or "name of presentation" in title_lower
            ):
                failures.append("presentation_powerpoint")

        # Сміттєві типи, які не є повноцінним джерелом
        doc_type = (details["doc_type"] or "other").lower()
        if doc_type in FRAGMENT_TYPES and page_count >= self.min_page_count:
            # Можемо пропустити тільки якщо достатньо сторінок; в іншому разі лічимо як фрагмент
            pass

        if failures:
            return QualityResult(passed=False, score=0.0, hard_failures=failures, details=details)

        # --- Оцінка якості 0..100 (тільки якщо пройшов жорсткі фільтри) ---
        score = 0.0

        # 1. Базовий бал за сторінки (максимум 40)
        page_score = min(40.0, page_count / 14 * 40.0)  # 14+ сторінок → повний бал
        score += page_score

        # 2. Тип документа (максимум 25)
        score += TYPE_BONUS.get(doc_type, TYPE_BONUS["other"])

        # 3. Наявність метаданих (максимум 20)
        meta = 0.0
        if doc.get("title"):
            meta += 5.0
        authors = doc.get("authors")
        if authors:
            meta += 5.0
        if doc.get("udc"):
            meta += 5.0
        if doc.get("year"):
            meta += 5.0
        score += meta

        # 4. Бонус за щільність тексту (максимум 15)
        if page_count > 0:
            chars_per_page = text_chars / page_count
            density = min(15.0, chars_per_page / 1500 * 15.0)  # 1500+/стор → повний бал
            score += density

        score = max(0.0, min(100.0, round(score, 1)))

        details.update(
            {
                "score": score,
                "chars_per_page": round(text_chars / page_count, 1) if page_count else 0.0,
            }
        )
        return QualityResult(passed=True, score=score, hard_failures=[], details=details)

    def rank(self, docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Профільтрувати та впорядкувати документи за якістю.

        Повертає список словників, кожен — копія вхідного документа з
        доданими ключами `_quality` (QualityResult) та `_quality_score`.
        """
        results: list[tuple[float, dict[str, Any]]] = []
        for doc in docs:
            qr = self.analyze(doc)
            if qr.passed:
                enriched = dict(doc)
                enriched["_quality"] = qr
                enriched["_quality_score"] = qr.score
                results.append((qr.score, enriched))
        results.sort(key=lambda t: t[0], reverse=True)
        return [enriched for _, enriched in results]


def parse_extra(doc: dict[str, Any]) -> dict[str, Any]:
    """Безпечно розпарсити поле extra документа."""
    extra = doc.get("extra")
    if isinstance(extra, dict):
        return extra
    if not isinstance(extra, str) or not extra.strip():
        return {}
    try:
        return json.loads(extra)
    except json.JSONDecodeError:
        return {}
