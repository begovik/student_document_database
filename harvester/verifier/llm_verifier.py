"""LLM-верифікація документа — Gemini 3.1 Flash Lite, 4 ключі GEMINI_DOC_VERIFIER_KEY_."""

from __future__ import annotations

import json

import structlog

from harvester.config import get_settings

logger = structlog.get_logger()

PROMPT = """\
Ти — верифікатор наукових джерел. ГОЛОВНА ЦІЛЬ: якісні джерела з повним текстом (титул, вступ/мета, розділи, висновки, список джерел). НЕ тези, НЕ зміст, НЕ анотації.

Документ:
- Заголовок: {title}
- Автори: {authors}
- Мова: {language}
- УДК: {udc}
- Сторінок: {page_count}
- Фрагмент: \"\"\"{text_sample}\"\"\"

Завдання: поверни ВИКЛЮЧНО JSON:
{{"verdict": "pass" | "fail", "comment": "1-2 речення укр чому", "confidence": 0.0-1.0}}
Правила:
- 1-2 стор без структури → fail ("фрагмент, відсутня структура")
- Немає вступу/висновків/списку джерел → fail
- Повна структура (титул, 3+ розділи, висновки, 5+ джерел) → pass
"""


async def verify_with_llm(doc: dict, llm_client) -> tuple[str, str, float]:
    """Викликати LLM для верифікації. Повертає (verdict, comment, confidence)."""
    title = doc.get("title") or doc.get("title_hint") or "невідомо"
    authors = doc.get("authors") or "невідомі"
    if isinstance(authors, list):
        authors = ", ".join(authors[:3])
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
        logger.info("verifier_llm_ok", doc_id=doc.get("id"), verdict=verdict, confidence=conf)
        return verdict, comment, conf
    except Exception as e:  # noqa: BLE001
        logger.warning("verifier_llm_error", doc_id=doc.get("id"), error=str(e)[:150])
        return "error", str(e)[:200], 0.0
