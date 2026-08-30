"""LLM-верифікація документа — Gemini 3.1 Flash Lite, 4 ключі GEMINI_DOC_VERIFIER_KEY_."""

from __future__ import annotations

import json

import structlog

logger = structlog.get_logger()

PROMPT = """\
Ти — верифікатор наукових джерел. ГОЛОВНА ЦІЛЬ: якісні джерела з повним текстом (титул, вступ/мета, розділи, висновки, список джерел). НЕ тези, НЕ зміст, НЕ анотації.

Документ (поточні метадані з БД):
- Заголовок: {title}
- Автори: {authors}
- Мова: {language}
- УДК: {udc}
- Сторінок: {page_count}
- Фрагмент тексту (до 3000 знаків): \"\"\"{text_sample}\"\"\"

Завдання: поверни ВИКЛЮЧНО JSON:
{{"verdict": "pass" | "fail", "comment": "1-2 речення укр чому", "confidence": 0.0-1.0, "extracted_title": "точна назва з титулу/першої сторінки або null", "extracted_authors": ["Прізвище І.О.", ...] | null}}
Правила для verdict:
- 1-2 стор без структури → fail ("фрагмент, відсутня структура")
- Немає вступу/висновків/списку джерел → fail
- Повна структура (титул, 3+ розділи, висновки, 5+ джерел) → pass
Для extracted_title/extracted_authors:
- Витягни точну назву та авторів з фрагменту (титул, шапка статті). Якщо автори є — перелічи всіх (до 5).
- Якщо в фрагменті немає авторів/назви — поверни null.
- Не вигадуй, бери лише з тексту.
"""


async def verify_with_llm(doc: dict, llm_client) -> tuple[str, str, float, str | None, list[str] | None]:
    """Викликати LLM для верифікації. Повертає (verdict, comment, confidence, extracted_title, extracted_authors)."""
    title = doc.get("title") or doc.get("title_hint") or "невідомо"
    authors_raw = doc.get("authors") or "невідомі"
    if isinstance(authors_raw, list):
        try:
            authors = ", ".join(authors_raw[:3]) if authors_raw else "невідомі"
            # Якщо authors зберігається як JSON-рядок
            if len(authors_raw) == 1 and isinstance(authors_raw[0], str) and authors_raw[0].startswith("["):
                authors = authors_raw[0]
        except Exception:
            authors = ", ".join(authors_raw[:3]) if isinstance(authors_raw, list) else str(authors_raw)
    else:
        authors = str(authors_raw)
    prompt = PROMPT.format(
        title=title,
        authors=authors,
        language=doc.get("language") or "невідома",
        udc=doc.get("udc") or "—",
        page_count=doc.get("page_count") or "?",
        text_sample=(doc.get("text_sample") or "")[:3000],
    )
    try:
        resp = await llm_client.complete(prompt)
        raw = resp.text.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw
            raw = raw.rsplit("```", 1)[0]
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start >= 0 and end > start:
            raw = raw[start:end]
        data = json.loads(raw)
        verdict = data.get("verdict", "fail")
        if verdict not in ("pass", "fail"):
            verdict = "fail"
        comment = str(data.get("comment") or "")[:300]
        conf = float(data.get("confidence") or 0.5)
        extracted_title = data.get("extracted_title")
        if isinstance(extracted_title, str):
            extracted_title = extracted_title.strip() or None
            if extracted_title and len(extracted_title) < 5:
                extracted_title = None
        else:
            extracted_title = None
        extracted_authors = data.get("extracted_authors")
        if not isinstance(extracted_authors, list):
            extracted_authors = None
        else:
            # Нормалізуємо авторів
            cleaned: list[str] = []
            for a in extracted_authors:
                if isinstance(a, str) and a.strip() and len(a.strip()) > 2:
                    cleaned.append(a.strip())
            extracted_authors = cleaned[:5] if cleaned else None

        logger.info(
            "verifier_llm_ok",
            doc_id=doc.get("id"),
            verdict=verdict,
            confidence=conf,
            extracted_title=(extracted_title[:60] if extracted_title else None),
            extracted_authors=extracted_authors,
        )
        return verdict, comment, conf, extracted_title, extracted_authors
    except Exception as e:  # noqa: BLE001
        logger.warning("verifier_llm_error", doc_id=doc.get("id"), error=str(e)[:150])
        return "error", str(e)[:200], 0.0, None, None
