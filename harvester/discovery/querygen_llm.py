"""LLM-генерація пошукових запитів — Gemini 3.1/3.5 Flash Lite (GEMINI_API_KEY 1-3) з fallback на Gemma."""

from __future__ import annotations

import json

import structlog

from harvester.config import get_settings

logger = structlog.get_logger()

# Головна ціль — якісні джерела з повним текстом, придатні для наукових праць
SYSTEM_PROMPT = """\
Ти — генератор пошукових запитів для наукової бібліотеки.
ГОЛОВНА ЦІЛЬ: збирати якісні джерела — документи з повним текстом (інформативні статті, монографії, посібники, придатні для наукових праць: титул, вступ/мета, розділи, висновки, список джерел). НЕ тези, НЕ зміст, НЕ анотації.

Завдання: для теми згенеруй 8-12 різноманітних пошукових запитів українською (можеш додати 2-3 англійських якщо тема має англомовні джерела).
Вимоги:
- Варіативність: синоніми, морфологія (пошиття/швейний/кравецький), пов'язані поняття (конструювання+розкрій+ВТО+потокове виробництво)
- Кожен запит має закінчуватись на filetype:pdf або містити "підручник filetype:pdf" / "навчальний посібник pdf"
- Не вигадуй неіснуючі терміни, не додавай .ru домени
- Поверни ЛИШЕ JSON: {{"queries": ["...", ...]}}

Приклад для "технологія пошиття пальта":
{{"queries": ["технологія виготовлення пальта filetype:pdf", "конструювання верхнього одягу пальто filetype:pdf", "розкрій та пошиття пальта навчальний посібник pdf", "sewing coat manufacturing technology filetype:pdf"]}}

Тема: {topic}
Існуючі запити (не дублюй): {existing}
Відповідь JSON:"""


def _parse_queries(text: str) -> list[str]:
    """Витягти JSON queries з відповіді LLM."""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text
        text = text.rsplit("```", 1)[0]
    start = text.find("{")
    end = text.rfind("}") + 1
    if start >= 0 and end > start:
        text = text[start:end]
    try:
        data = json.loads(text)
        qs = data.get("queries") if isinstance(data, dict) else None
        if isinstance(qs, list):
            out: list[str] = []
            for q in qs:
                if isinstance(q, str) and len(q.strip()) > 10:
                    # Легкий фільтр RU
                    if ".ru" in q.lower() or "xn--p1ai" in q.lower():
                        continue
                    out.append(q.strip())
            return out[:12]
    except Exception as e:  # noqa: BLE001
        logger.warning("querygen_llm_parse_error", error=str(e)[:100], raw=text[:200])
    return []


async def generate_queries_for_topic(
    topic_name: str,
    existing_queries: list[str] | None = None,
    count: int = 10,
) -> list[str]:
    """Згенерувати запити LLM-ом: Gemini 3.1/3.5 Flash Lite (GEMINI_API_KEY 1-3) → fallback Gemma."""
    settings = get_settings()
    if not settings.llm.enabled:
        return []
    if not settings.gemini_keys and not settings.open_router_api_key:
        logger.warning("querygen_llm_no_keys")
        return []

    # Обмеження за лімітами: Gemini 3.1: 2/15 RPM, 36.5K/250K TPM, 500 RPD
    # Один виклик ~ 1.5k токенів вхід + 0.5k вихід < 2k, тому 2 RPM = 30с на запит при 1 ключі, 10с при 3 ключах
    prompt = SYSTEM_PROMPT.format(
        topic=topic_name,
        existing=", ".join((existing_queries or [])[:5]) or "немає",
    )

    try:
        from harvester.classify.llm import AllLimitsExhausted, LLMClient, LLMUnavailable

        client = LLMClient(keys=settings.gemini_keys, service="QueryGen")
        response = await client.complete(prompt)
        queries = _parse_queries(response.text)
        if queries:
            logger.info(
                "querygen_llm_ok",
                provider=response.provider,
                model=response.model,
                topic=topic_name[:40],
                count=len(queries),
            )
        return queries[: max(1, count)]
    except (LLMUnavailable, AllLimitsExhausted) as e:
        logger.warning("querygen_llm_unavailable", topic=topic_name[:40], error=str(e)[:200])
        return []
    except Exception as e:  # noqa: BLE001
        logger.warning("querygen_llm_error", topic=topic_name[:40], error=str(e)[:200])
        return []
