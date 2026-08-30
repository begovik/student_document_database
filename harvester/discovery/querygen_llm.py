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
    keys = settings.gemini_keys  # лише GEMINI_API_KEY 1-3
    if not keys:
        logger.warning("querygen_llm_no_keys")
        return []

    # Обмеження за лімітами: Gemini 3.1: 2/15 RPM, 36.5K/250K TPM, 500 RPD
    # Один виклик ~ 1.5k токенів вхід + 0.5k вихід < 2k, тому 2 RPM = 30с на запит при 1 ключі, 10с при 3 ключах
    prompt = SYSTEM_PROMPT.format(
        topic=topic_name,
        existing=", ".join((existing_queries or [])[:5]) or "немає",
    )

    # Спроба 1: Gemini 3.1/3.5 Flash Lite напряму
    for model in settings.llm.gemini_models:
        for key in keys:
            try:
                text = await _call_gemini(prompt, key, model)
                if text:
                    qs = _parse_queries(text)
                    if qs:
                        logger.info("querygen_llm_ok", model=model, topic=topic_name[:40], count=len(qs))
                        return qs[:count]
            except Exception as e:  # noqa: BLE001
                err = str(e)
                is_quota = "429" in err and ("quota" in err.lower() or "exceeded" in err.lower())
                is_rate = "429" in err
                if is_quota:
                    logger.warning("querygen_llm_quota", model=model, error=err[:100])
                    continue
                if is_rate:
                    logger.warning("querygen_llm_rate", model=model, error=err[:100])
                    continue
                logger.warning("querygen_llm_error", model=model, error=err[:150])

    # Fallback: Gemma 4 31b/26b-a4b (ті ж 3 ключі, стиснений промпт)
    try:
        from harvester.classify.llm import rephrase_for_gemma
    except Exception:
        return []

    for model in settings.llm.gemma_models:
        for key in keys:
            try:
                short_prompt = rephrase_for_gemma(prompt, settings.llm.gemma_max_chars)
                text = await _call_gemini(prompt=short_prompt, key=key, model=model)
                if text:
                    qs = _parse_queries(text)
                    if qs:
                        logger.info("querygen_llm_ok", model=model, topic=topic_name[:40], count=len(qs))
                        return qs[:count]
            except Exception as e:  # noqa: BLE001
                logger.warning("querygen_llm_gemma_error", model=model, error=str(e)[:150])

    logger.warning("querygen_llm_all_failed", topic=topic_name[:40])
    return []


async def _call_gemini(prompt: str, key: str, model: str) -> str | None:
    """Низькорівневий виклик Gemini generateContent."""
    import httpx

    settings = get_settings()
    cfg = settings.llm
    url = f"{cfg.gemini_base_url}/models/{model}:generateContent"
    async with httpx.AsyncClient(timeout=cfg.timeout_s) as client:
        resp = await client.post(
            url,
            params={"key": key},
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": cfg.temperature, "maxOutputTokens": 1024},
            },
        )
        if resp.status_code == 429:
            raise RuntimeError(f"429 {resp.text[:200]}")
        resp.raise_for_status()
        data = resp.json()
        parts = data["candidates"][0]["content"]["parts"]
        text = ""
        for p in parts:
            if not p.get("thought", False):
                text = p.get("text", "")
                break
        if not text:
            text = parts[-1].get("text", "")
        return text.strip() or None
