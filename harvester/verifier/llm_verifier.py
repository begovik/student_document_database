"""LLM-верифікація документа — Gemini 3.1 Flash Lite, 4 ключі GEMINI_DOC_VERIFIER_KEY_."""

from __future__ import annotations

import json

import structlog

logger = structlog.get_logger()

DOC_TYPES = ["article", "book", "textbook", "methodical", "thesis", "dissertation", "report", "preprint", "other"]

PROMPT = """\
Ти — верифікатор наукових джерел. ГОЛОВНА ЦІЛЬ: якісні джерела з повним текстом (титул, вступ/мета, розділи, висновки, список джерел). НЕ тези, НЕ зміст, НЕ анотації.

Документ (поточні метадані з БД):
- Заголовок: {title}
- Автори: {authors}
- Мова: {language}
- УДК: {udc}
- Сторінок: {page_count}
- Фрагмент тексту (до 3000 знаків): \"\"\"{text_sample}\"\"\"

Доступні теги (коди тем, обери 1-3):
{topics_list}
Доступні типи документу: {doc_types}

Завдання: поверни ВИКЛЮЧНО JSON:
{{"verdict": "pass" | "fail", "comment": "1-2 речення укр чому", "confidence": 0.0-1.0, "extracted_title": "точна назва з титулу/першої сторінки або null", "extracted_authors": ["Прізвище І.О.", ...] | null, "tags": ["код_теми", ...], "doc_type": "тип"}}
Правила для verdict:
- 1-2 стор без структури → fail ("фрагмент, відсутня структура")
- Немає вступу/висновків/списку джерел → fail
- Повна структура (титул, 3+ розділи, висновки, 5+ джерел) → pass
Для extracted_title/extracted_authors:
- Витягни точну назву та авторів з фрагменту (титул, шапка статті). Якщо автори є — перелічи всіх (до 5).
- Якщо в фрагменті немає авторів/назви — поверни null.
- Не вигадуй, бери лише з тексту.
Для tags:
- Обери 1-3 коди з переліку вище за змістом фрагменту. Якщо жодна не підходить — [].
Для doc_type: обери один з {doc_types} за змістом (article — стаття, book — монографія/книга, textbook — підручник, methodical — метод. вказівки, thesis — диплом/магістерська, dissertation — дисертація/автореферат дисертації, report — звіт/тези доповіді, preprint — препринт, other — інше). Якщо не впевнений — other.
"""


async def verify_with_llm(doc: dict, llm_client, topics: list[dict] | None = None) -> tuple[str, str, float, str | None, list[str] | None, list[str], str]:
    """Викликати LLM для верифікації. Повертає (verdict, comment, confidence, extracted_title, extracted_authors, tags, doc_type)."""
    title = doc.get("title") or doc.get("title_hint") or "невідомо"
    authors_raw = doc.get("authors") or "невідомі"
    if isinstance(authors_raw, list):
        try:
            authors = ", ".join(authors_raw[:3]) if authors_raw else "невідомі"
            if len(authors_raw) == 1 and isinstance(authors_raw[0], str) and authors_raw[0].startswith("["):
                authors = authors_raw[0]
        except Exception:
            authors = ", ".join(authors_raw[:3]) if isinstance(authors_raw, list) else str(authors_raw)
    else:
        authors = str(authors_raw)

    # Динамічний список тем для тегів
    if topics is None:
        topics = []
    topics_list = "\n".join(f"- {t['code']} — {t['name_uk']} / {t['name_en']}" for t in topics) or "немає тем"
    prompt = PROMPT.format(
        title=title,
        authors=authors,
        language=doc.get("language") or "невідома",
        udc=doc.get("udc") or "—",
        page_count=doc.get("page_count") or "?",
        text_sample=(doc.get("text_sample") or "")[:3000],
        topics_list=topics_list,
        doc_types=", ".join(DOC_TYPES),
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
            cleaned: list[str] = []
            for a in extracted_authors:
                if isinstance(a, str) and a.strip() and len(a.strip()) > 2:
                    cleaned.append(a.strip())
            extracted_authors = cleaned[:5] if cleaned else None

        # Теги та тип
        tags = data.get("tags") if isinstance(data.get("tags"), list) else []
        tags = [str(t).strip() for t in tags if isinstance(t, str) and t.strip()][:3]
        # Фільтруємо лише валідні коди тем
        if topics:
            valid_codes = {t["code"] for t in topics}
            tags = [t for t in tags if t in valid_codes][:3]
        doc_type = str(data.get("doc_type") or "other").strip().lower()
        if doc_type not in DOC_TYPES:
            doc_type = "other"

        logger.info(
            "verifier_llm_ok",
            doc_id=doc.get("id"),
            verdict=verdict,
            confidence=conf,
            extracted_title=(extracted_title[:60] if extracted_title else None),
            extracted_authors=extracted_authors,
            tags=tags,
            doc_type=doc_type,
        )
        return verdict, comment, conf, extracted_title, extracted_authors, tags, doc_type
    except Exception as e:  # noqa: BLE001
        logger.warning("verifier_llm_error", doc_id=doc.get("id"), error=str(e)[:150])
        return "error", str(e)[:200], 0.0, None, None, [], "other"
