"""LLM-відбір документів за темою."""

from __future__ import annotations

import json
from typing import Any

import structlog

from harvester.config import FilterRules, get_filter_rules, get_settings
from harvester.curator.prompts import (
    CANDIDATE_LINE,
    PROMPT_SELECT_END,
    get_selection_prompt,
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
    rules: FilterRules | None = None,
    min_count: int | None = None,
) -> SelectionResult | None:
    """Викликати LLM для відбору документів.

    Returns SelectionResult або None за помилки.
    """
    settings = get_settings()
    if not settings.llm.enabled:
        logger.warning("llm_disabled_skip_selection")
        return None
    
    if rules is None:
        rules = get_filter_rules()
    
    # Отримати промпт залежно від рівня строгості
    prompt_template = get_selection_prompt(rules.llm_completeness_level)
    
    # Створити текст промпта
    prompt = prompt_template.format(topic=topic, count=len(candidates))
    for c in candidates:
        prompt += CANDIDATE_LINE.format(
            id=c["id"],
            title=c.get("title", "(без назви)")[:80],
            authors=c.get("authors", "(немає)")[:60],
            year=c.get("year", "?") or "?",
            page_count=c.get("page_count", "?") or "?",
            doc_type=c.get("doc_type", "unknown"),
            topic_score=c.get("topic_score", 0.0),
        )
    prompt += PROMPT_SELECT_END.replace("{min_count}", str(min_count or 10))

    try:
        from harvester.classify.llm import LLMClient, LLMUnavailable

        client = LLMClient(keys=settings.gemini_keys, service="CuratorSelect")
        response = await client.complete(prompt)
        result = parse_selection_response(response.text)
        if result is None:
            logger.warning("selection_invalid_response", topic=topic, provider=response.provider)
            return None
        valid_ids = {int(c["id"]) for c in candidates if c.get("id") is not None}
        result.selected_ids = [doc_id for doc_id in result.selected_ids if doc_id in valid_ids]
        result = enforce_min_count(result, candidates, min_count)
        result.topic = topic
        result.candidates_count = len(candidates)
        logger.info(
            "selection_success",
            topic=topic,
            provider=response.provider,
            count=result.suggested_count,
            selected=len(result.selected_ids),
        )
        return result
    except LLMUnavailable as e:
        logger.warning("selection_llm_unavailable", topic=topic, error=str(e)[:300])
    except Exception as e:  # noqa: BLE001
        logger.error("selection_unexpected_error", topic=topic, error_msg=str(e)[:200])
    return None


def parse_selection_response(text: str) -> SelectionResult | None:
    """Парсити відповідь LLM на відбір документів."""
    decoder = json.JSONDecoder()
    data = None
    for index, char in enumerate(text):
        if char != "{":
            continue
        try:
            candidate, _ = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(candidate, dict) and "selected_ids" in candidate:
            data = candidate
            break
    if not isinstance(data, dict):
        return None

    suggested_count = data.get("suggested_count", 30)
    if isinstance(suggested_count, bool):
        suggested_count = 30
    try:
        suggested_count = int(suggested_count)
    except (TypeError, ValueError):
        suggested_count = 30
    selected_ids = data.get("selected_ids", [])
    reasoning = data.get("reasoning", "")

    if not isinstance(selected_ids, list):
        selected_ids = []
    normalized_ids: list[int] = []
    for value in selected_ids:
        if isinstance(value, bool):
            continue
        try:
            doc_id = int(value)
        except (TypeError, ValueError):
            continue
        if doc_id > 0 and doc_id not in normalized_ids:
            normalized_ids.append(doc_id)
    selected_ids = normalized_ids

    # Обмежити діапазон
    suggested_count = max(suggested_count, 20)
    suggested_count = min(suggested_count, 50)
    if len(selected_ids) > suggested_count:
        selected_ids = selected_ids[:suggested_count]

    return SelectionResult(
        topic="",
        candidates_count=0,
        suggested_count=suggested_count,
        selected_ids=selected_ids,
        reasoning=str(reasoning),
    )


def enforce_min_count(
    result: SelectionResult,
    candidates: list[dict[str, Any]],
    min_count: int | None,
) -> SelectionResult:
    """Підняти кількість обраних документів до мінімуму, якщо кандидатів вистачає."""
    if not min_count:
        return result

    selected_ids = list(result.selected_ids)
    suggested_count = result.suggested_count

    if suggested_count < min_count and len(candidates) >= min_count:
        selected_set = set(selected_ids)
        for c in candidates:
            if len(selected_ids) >= min_count:
                break
            if c["id"] not in selected_set:
                selected_ids.append(c["id"])
                selected_set.add(c["id"])
        suggested_count = min(min_count, len(candidates))

    return SelectionResult(
        topic=result.topic,
        candidates_count=result.candidates_count or len(candidates),
        suggested_count=suggested_count,
        selected_ids=selected_ids,
        reasoning=result.reasoning,
    )


def format_candidates_text(
    candidates: list[dict[str, Any]],
    topic: str,
) -> str:
    """Сформатувати текст для LLM-промпта відбору."""
    prompt = get_selection_prompt("basic").format(topic=topic, count=len(candidates))
    for c in candidates:
        prompt += CANDIDATE_LINE.format(
            id=c["id"],
            title=c.get("title", "(без назви)")[:80],
            authors=c.get("authors", "(немає)")[:60],
            year=c.get("year", "?") or "?",
            page_count=c.get("page_count", "?") or "?",
            doc_type=c.get("doc_type", "unknown"),
            topic_score=c.get("topic_score", 0.0),
        )
    prompt += PROMPT_SELECT_END.replace("{min_count}", "10")
    return prompt
