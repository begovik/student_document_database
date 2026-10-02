"""LLM-верифікація документа — Gemini 3.1 Flash Lite, 4 ключі GEMINI_DOC_VERIFIER_KEY_."""

from __future__ import annotations

import json

import structlog

from harvester.classify.llm import AllLimitsExhausted, log_raw_response, redact_secrets

logger = structlog.get_logger()

MIN_LLM_CONFIDENCE = 0.5

# Скільки тексту реально показувати LLM. Раніше тут було жорстко 3000
# знаків, а `llm_max_chars` з конфігу не використовувався ніде — тобто
# налаштування мовчки нічого не робило.
#
# Чому 3000 було недостатньо: українська наукова стаття відкривається
# УДК + назвою + анотацією (200-400 слів) + ключовими словами, тобто
# перші ~2500 знаків — це титул і анотація. LLM, бачивши лише їх,
# писав у вердикті «наданий фрагмент є лише анотацією» — і відхиляв
# повні статті. Виміряно на 16 тематичних правових документах: усі
# відхилені з таким коментарем мали рівно цю причину.
#
# 12000 знаків — це приблизно 6-8 сторінок, тобто вже видно основний
# текст, а не лише аннотацію.
DEFAULT_LLM_MAX_CHARS = 12000

DOC_TYPES = ["article", "book", "textbook", "methodical", "thesis", "dissertation", "report", "preprint", "other"]

PROMPT = """\
Ти — верифікатор наукових джерел. ГОЛОВНА ЦІЛЬ: якісні джерела з повним текстом (титул, вступ/мета, розділи, висновки, список джерел). НЕ тези, НЕ зміст, НЕ анотації.

Документ (поточні метадані з БД):
- Заголовок: {title}
- Автори: {authors}
- Мова: {language}
- УДК: {udc}
- Сторінок: {page_count}
- Початковий фрагмент тексту (до {max_chars} знаків; це НЕ весь документ): \"\"\"{text_sample}\"\"\"
- Структурні ознаки, обчислені парсером усього PDF: {structure}
- Чи зібрані структурні ознаки: {structure_missing}

Доступні теги (коди тем, обери 1-3):
{topics_list}
Доступні типи документу: {doc_types}

Завдання: поверни ВИКЛЮЧНО JSON:
{{"verdict": "pass" | "fail", "comment": "1-2 речення укр чому", "confidence": 0.0-1.0, "extracted_title": "точна назва з титулу/першої сторінки або null", "extracted_authors": ["Прізвище І.О.", ...] | null, "tags": ["код_теми", ...], "doc_type": "тип"}}
Правила для verdict:
- 1-2 стор без структури → fail ("фрагмент, відсутня структура")
- Дипломна робота, автореферат дисертації або інша нецільова праця → fail
- Повна структура (титул, 3+ розділи, висновки, 5+ джерел) → pass
- Впевненість у pass нижче {min_confidence} → fail; не вигадуй дані

ВАЖЛИВО — не плутай відсутнє у фрагменті з відсутнім у документі:
Фрагмент — це лише початок тексту. Ти НЕ бачиш кінця документа, де
зазвичай і лежать висновки та список джерел. Тому:
- якщо у фрагменті немає висновків або списку джерел — це НЕ підстава для fail;
- якщо `Чи зібрані структурні ознаки` = "ТАК" — це означає, що парсер не
  відпрацював (документ перевірено до впровадження цієї перевірки), і
  порожня структура НЕ довід відсутності розділів;
- підстава для fail — коли видно, що документ обривається: лише титул
  і анотація без жодного основного тексту, або 1-2 сторінки;
- спирайся на `Сторінок`: 3+ сторінки з непустим основним текстом у
  фрагменті — це сильний доказ на користь повноцінної праці.
Для extracted_title/extracted_authors:
- Витягни точну назву та авторів з фрагменту (титул, шапка статті). Якщо автори є — перелічи всіх (до 5).
- Якщо в фрагменті немає авторів/назви — поверни null.
- Не вигадуй, бери лише з тексту.
Для tags:
- Обери 1-3 коди з переліку вище за змістом фрагменту. Якщо жодна не підходить — [].
Для doc_type: обери один з {doc_types} за змістом (article — стаття, book — монографія/книга, textbook — підручник, methodical — метод. вказівки, thesis — диплом/магістерська, dissertation — дисертація/автореферат дисертації, report — звіт/тези доповіді, preprint — препринт, other — інше). Якщо не впевнений — other.
"""


def _max_chars() -> int:
    """Скільки символів тексту показувати LLM (verifier.llm_max_chars)."""
    try:
        from harvester.config import get_settings

        return get_settings().verifier.llm_max_chars
    except Exception:  # noqa: BLE001 — конфіг не критичний для промпта
        return DEFAULT_LLM_MAX_CHARS


async def verify_with_llm(doc: dict, llm_client, topics: list[dict] | None = None) -> tuple[str, str, float, str | None, list[str] | None, list[str], str]:
    """Викликати LLM для верифікації. Повертає (verdict, comment, confidence, extracted_title, extracted_authors, tags, doc_type)."""
    title = doc.get("title") or doc.get("title_hint") or "невідомо"
    authors_raw = doc.get("authors") or "невідомі"
    if isinstance(authors_raw, list):
        try:
            authors = ", ".join(authors_raw[:3]) if authors_raw else "невідомі"
            if len(authors_raw) == 1 and isinstance(authors_raw[0], str) and authors_raw[0].startswith("["):
                authors = authors_raw[0]
        except Exception as e:  # noqa: BLE001
            # authors у БД — не список. Fallback дає моделі некоректне
            # представлення авторів, тож вердикт може бути спотворений.
            logger.debug(
                "verifier_authors_format_invalid",
                doc_id=doc.get("id"),
                got_type=type(authors_raw).__name__,
                error=str(e)[:120],
            )
            authors = ", ".join(authors_raw[:3]) if isinstance(authors_raw, list) else str(authors_raw)
    else:
        authors = str(authors_raw)

    # Динамічний список тем для тегів
    if topics is None:
        topics = []
    topics_list = "\n".join(f"- {t['code']} — {t['name_uk']} / {t['name_en']}" for t in topics) or "немає тем"
    extra = doc.get("extra")
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except json.JSONDecodeError:
            extra = {}
    structure = extra.get("structure") if isinstance(extra, dict) else None
    # Документи, перевірені до появи `structure` у extra, не мають цього
    # поля взагалі. Для них відсутність структурних ознак НЕ є доказом
    # відсутності структури, і промпт має це сказати прямо — інакше LLM
    # читає порожній словник як «розділів немає».
    structure_missing = not structure
    if structure_missing:
        structure = {}

    max_chars = _max_chars()

    prompt = PROMPT.format(
        title=title,
        authors=authors,
        language=doc.get("language") or "невідома",
        udc=doc.get("udc") or "—",
        page_count=doc.get("page_count") or "?",
        text_sample=(doc.get("text_sample") or "")[:max_chars],
        max_chars=max_chars,
        structure=json.dumps(structure, ensure_ascii=False, sort_keys=True)[:1500],
        structure_missing="ТАК — структурні ознаки НЕ ЗІБРАНО" if structure_missing else "ні",
        topics_list=topics_list,
        doc_types=", ".join(DOC_TYPES),
        min_confidence=MIN_LLM_CONFIDENCE,
    )
    try:
        resp = await llm_client.complete(prompt)
        raw = resp.text.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw
            raw = raw.rsplit("```", 1)[0]
        decoder = json.JSONDecoder()
        data = None
        for start, char in enumerate(raw):
            if char != "{":
                continue
            try:
                candidate, _ = decoder.raw_decode(raw[start:])
            except json.JSONDecodeError:
                continue
            if isinstance(candidate, dict):
                data = candidate
                break
        if data is None:
            # Обов'язково пишемо сиру відповідь: без неї «не JSON-об'єкт»
            # не відрізняє обрізаний вивід, thinking-бюджет і HTML-помилку.
            # Саме через відсутність цього логу дефект thinking-моделі
            # протримався непомітним і мовчки псував якість пулу.
            log_raw_response(logger, "verifier_llm_no_json", resp, doc_id=doc.get("id"))
            return "error", "LLM не повернув JSON-об'єкт", 0.0, None, None, [], "other"
        verdict = data.get("verdict", "fail")
        if verdict not in ("pass", "fail"):
            verdict = "fail"
        comment = str(data.get("comment") or "")[:300]
        try:
            conf = float(data.get("confidence") or 0.5)
        except (TypeError, ValueError):
            conf = 0.0
        conf = max(0.0, min(1.0, conf))
        if verdict == "pass" and conf < MIN_LLM_CONFIDENCE:
            verdict = "fail"
            comment = comment or "LLM не досяг достатньої впевненості"
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

        # Повний лог вердикту пише worker (він знає doc_id і контекст батчу);
        # тут лишаємо лише рівень debug, щоб не дублювати подію двічі.
        logger.debug(
            "verifier_llm_parsed",
            verdict=verdict,
            confidence=conf,
            tags=tags,
            doc_type=doc_type,
        )
        return verdict, comment, conf, extracted_title, extracted_authors, tags, doc_type
    except AllLimitsExhausted:
        # Добове вичерпання квоти не є помилкою якості документа: дозволити
        # worker-у відкласти батч і спробувати знову після відновлення.
        raise
    except Exception as e:  # noqa: BLE001
        # Тип помилки потрібен для діагностики: ValueError від парсера JSON
        # означає «формат відповіді», httpx/HTTPStatusError — «провайдер».
        logger.warning(
            "verifier_llm_error",
            doc_id=doc.get("id"),
            error=redact_secrets(str(e))[:150],
            error_type=type(e).__name__,
        )
        return "error", str(e)[:200], 0.0, None, None, [], "other"
