"""Тонка обгортка над існуючими strict-правилами — без дублювання."""

from __future__ import annotations

from typing import Any

from harvester.config import get_filter_rules


def check_strict_rules(doc: dict[str, Any]) -> tuple[bool, list[str], str]:
    """Перевірити документ strict-правилами, не створюючи нових.

    Використовує:
    - is_document_complete() з harvester/curator/preparer.py (title/authors/page_count/has_text_layer)
    - pdf_quality + RU-фільтри вже враховані в is_document_complete/filter

    Повертає: (passed, failed_rules, comment)
    """
    # Імпорт тут щоб уникнути циклу
    from harvester.curator.preparer import is_document_complete

    rules = get_filter_rules("strict")
    passed, reason = is_document_complete(doc, rules)
    if not passed:
        return False, [reason or "unknown"], reason or "не відповідає strict-правилам"

    # Додатково: якщо документ має extra.producer PPT — вже відфільтровано в is_document_complete,
    # але для коментаря додамо
    return True, [], "відповідає strict-правилам: титул, автори, сторінки >=3, текстовий шар"
