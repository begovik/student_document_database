"""LLM-відбір документів за темою."""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any

import structlog

from harvester.config import get_settings
from harvester.curator.prompts import (
    CANDIDATE_LINE,
    PROMPT_SELECT_DOCUMENTS,
    PROMPT_SELECT_END,
)

logger = structlog.get_logger()


class SelectionResult:
    """Результат роботи LLM-відбору."""

    def __init__(
        self,
        topic: str,
        candidates_count: int,
        suggested_count: int,
        selected_ids: list[int],
        reasoning: str,
    ):
        self.topic = topic
        self.candidates_count = candidates_count
        self.suggested_count = suggested_count
        self.selected_ids = selected_ids
        self.reasoning = reasoning

    def summary(self) -> str:
        return f"{self.topic}: {self.suggested_count}/{self.candidates_count} обраних, {len(self.selected_ids)} ID"


async def call_llm_for_selection(
    topic: str,
    candidates: list[dict[str, Any]],
) -> SelectionResult | None:
    """Викликати LLM для відбору документів.

    Returns SelectionResult або None за помилки.
    """
    settings = get_settings()
    if not settings.llm.enabled:
        logger.warning("llm_disabled_skip_selection")
        return None

    # Створити текст промпта
    prompt = PROMPT_SELECT_DOCUMENTS.format(topic=topic, count=len(candidates))
    for c in candidates:
        prompt += CANDIDATE_LINE.format(
            id=c["id"],
            title=c.get("title", "(без назви)")[:80],
            authors=c.get("authors", "(немає)")[:60],
            year=c.get("year", "?") or "?",
            doc_type=c.get("doc_type", "unknown"),
            topic_score=c.get("topic_score", 0.0),
        )
    prompt += PROMPT_SELECT_END

    # Виклик LLM
    try:
        import aiohttp

        config = settings.llm
        gemini_keys = [
            settings.gemini_api_key,
            settings.gemini_api_key_2,
            settings.gemini_api_key_3,
        ]
        gemini_keys = [k for k in gemini_keys if k]

        models = config.gemini_models or ["gemini-3.1-flash-lite"]

        last_error: Exception | None = None
        used_model = None

        # Фаза 1: Gemini
        for model in models:
            for gemini_key in gemini_keys:
                try:
                    used_model = model
                    client = aiohttp.ClientSession()
                    url = (
                        f"{config.gemini_base_url}/models/{model}:generateContent"
                        f"?key={gemini_key}"
                    )
                    payload = {
                        "contents": [
                            {
                                "parts": [
                                    {"text": prompt},
                                ]
                            }
                        ],
                        "generationConfig": {
                            "temperature": config.temperature,
                            "maxOutputTokens": config.max_tokens,
                        },
                    }
                    async with client:
                        resp = await client.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=config.timeout_s))
                        if resp.status != 200:
                            body = await resp.text()
                            raise RuntimeError(f"Gemini API error {resp.status}: {body[:300]}")

                        data = await resp.json()
                        text = ""
                        for part in data.get("candidates", [{}])[0].get("content", {}).get("parts", []):
                            text += part.get("text", "")
                        if not text:
                            raise RuntimeError("Порожня відповідь від LLM")

                        result = parse_selection_response(text)
                        if result:
                            await client.close()
                            logger.info("selection_success", topic=topic, count=result.suggested_count, selected=len(result.selected_ids))
                            return result
                        else:
                            raise RuntimeError("Не вдалося парсити відповідь LLM")

                except Exception as e:
                    await client.close()
                    last_error = e
                    logger.warning("llm_selection_attempt_failed", model=model, key=gemini_key[:8] + "...", error=str(e)[:100])
                    await asyncio.sleep(config.min_interval_s)

        # Фаза 2: Gemma (ті самі ключі, gemma_models + стиснення)
        from harvester.classify.llm import rephrase_for_gemma

        gemma_models = config.gemma_models or ["gemma-4-31b-it", "gemma-4-26b-it"]
        truncated_prompt = rephrase_for_gemma(prompt, config.gemma_max_chars)

        for model in gemma_models:
            for gemini_key in gemini_keys:
                try:
                    used_model = model
                    client = aiohttp.ClientSession()
                    url = (
                        f"{config.gemini_base_url}/models/{model}:generateContent"
                        f"?key={gemini_key}"
                    )
                    payload = {
                        "contents": [
                            {
                                "parts": [
                                    {"text": truncated_prompt},
                                ]
                            }
                        ],
                        "generationConfig": {
                            "temperature": config.temperature,
                            "maxOutputTokens": config.max_tokens,
                        },
                    }
                    async with client:
                        resp = await client.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=config.timeout_s))
                        if resp.status != 200:
                            body = await resp.text()
                            raise RuntimeError(f"Gemma API error {resp.status}: {body[:300]}")

                        data = await resp.json()
                        text = ""
                        for part in data.get("candidates", [{}])[0].get("content", {}).get("parts", []):
                            text += part.get("text", "")
                        if not text:
                            raise RuntimeError("Порожня відповідь від LLM")

                        result = parse_selection_response(text)
                        if result:
                            await client.close()
                            logger.info("selection_success_gemma", topic=topic, model=model, count=result.suggested_count, selected=len(result.selected_ids))
                            return result
                        else:
                            raise RuntimeError("Не вдалося парсити відповідь LLM")

                except Exception as e:
                    await client.close()
                    last_error = e
                    logger.warning("gemma_selection_attempt_failed", model=model, key=gemini_key[:8] + "...", error=str(e)[:100])
                    await asyncio.sleep(config.min_interval_s)

        logger.error("selection_all_attempts_failed", topic=topic, error=str(last_error)[:200])
        return None

    except Exception as e:
        last_error = e
        logger.error("selection_unexpected_error", topic=topic, error=str(e)[:200])
        return None


def parse_selection_response(text: str) -> SelectionResult | None:
    """Парсити відповідь LLM на відбір документів."""
    # Прибраний markdown- fences
    text = text.replace("```json", "").replace("```", "").replace("```json", "")

    # Знайти JSON у тексті
    match = re.search(r'\{[^}]*"suggested_count"[^}]*\}', text)
    if not match:
        match = re.search(r'\{.*\}', text)
    if not match:
        return None

    try:
        data = json.loads(match.group())
    except json.JSONDecodeError:
        return None

    suggested_count = data.get("suggested_count", 30)
    selected_ids = data.get("selected_ids", [])
    reasoning = data.get("reasoning", "")

    if not isinstance(selected_ids, list):
        selected_ids = []
    selected_ids = [int(x) for x in selected_ids if isinstance(x, (int, float)) and x > 0]

    # Обмежити діапазон
    if suggested_count < 20:
        suggested_count = 20
    if suggested_count > 50:
        suggested_count = 50
    if len(selected_ids) > suggested_count:
        selected_ids = selected_ids[:suggested_count]
    if len(selected_ids) == 0 and suggested_count <= len(list(candidates)):
        # Якщо модель не вказала ID, але знала кількість — обираємо перші N
        pass

    return SelectionResult(
        topic="",
        candidates_count=0,
        suggested_count=suggested_count,
        selected_ids=selected_ids,
        reasoning=reasoning,
    )


def format_candidates_text(
    candidates: list[dict[str, Any]],
    topic: str,
) -> str:
    """Сформатувати текст для LLM-промпта відбору."""
    prompt = PROMPT_SELECT_DOCUMENTS.format(topic=topic, count=len(candidates))
    for c in candidates:
        prompt += CANDIDATE_LINE.format(
            id=c["id"],
            title=c.get("title", "(без назви)")[:80],
            authors=c.get("authors", "(немає)")[:60],
            year=c.get("year", "?") or "?",
            doc_type=c.get("doc_type", "unknown"),
            topic_score=c.get("topic_score", 0.0),
        )
    prompt += PROMPT_SELECT_END
    return prompt
