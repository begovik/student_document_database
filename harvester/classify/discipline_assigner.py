"""DisciplineAssigner — цілодобове присвоювання дисциплін каталогу.

Verified-документи отримують topic-рядки kind='discipline' за реальною
відповідністю змісту (LLM Gemini 3.5 Flash Lite, ключі
GEMINI_DOC_VERIFIER_KEY_1..4). Документ маркується discipline_checked_at
отримує незалежно від того, знайдено дисципліни чи ні.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import structlog

from harvester.classify.llm import AllLimitsExhausted, LLMClient
from harvester.config import get_settings
from harvester.db.failover import build_database

logger = structlog.get_logger()

DISCIPLINE_ASSIGN_PROMPT = """Ти — бібліотекар-таксономіст. Визнач, до яких наукових дисциплін належить документ.

ПРАВИЛА:
- Присвоюй дисципліну ЛИШЕ якщо документ справді присвячений їй: тематика і зміст відповідають.
- Не приписуй дисципліни «за вуха»: випадкові згадки, списки літератури або вступні слова про суміжні галузі не рахуються.
- Повертай 1-3 дисципліни зі списку за релевантністю; якщо жодна не підходить — порожній список.
- Confidence — впевненість у присвоєнні (0..1).

ДОКУМЕНТ:
- Заголовок: {title}
- Автори: {authors}
- Мова: {language}
- УДК: {udc}
- Тип: {doc_type}
- Фрагмент тексту:
\"\"\"
{text_sample}
\"\"\"

ДОСТУПНІ ДИСЦИПЛІНИ (код — назва):
{disciplines_list}

ВІДПОВІДЬ — ВИКЛЮЧНО валідний JSON без пояснень:
{{
  "disciplines": ["код1", "код2"],
  "confidence": 0.0
}}"""


def _tomorrow_midnight_utc() -> datetime:
    now = datetime.now(UTC)
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return tomorrow


def _utcnow() -> datetime:
    """Наївний UTC-час для isoformat (без суфікса +00:00) — для порівнянь у SQL."""
    return datetime.now(UTC).replace(tzinfo=None)


def _extract_json(raw: str) -> dict:
    """Витягти JSON-об'єкт з відповіді LLM (з markdown-блоків та `thinking`)."""
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1] if "\n" in raw else raw
        raw = raw.rsplit("```", 1)[0]
    json_start = raw.find("{")
    json_end = raw.rfind("}") + 1
    if json_start >= 0 and json_end > json_start:
        raw = raw[json_start:json_end]
    return json.loads(raw)


class DisciplineAssigner:
    """Цикл присвоювання дисциплін для verified-документів (Gemini 3.5 Flash Lite)."""

    def __init__(self, worker_id: int = 0):
        self.worker_id = worker_id
        self.settings = get_settings()
        cfg = self.settings.discipline_assign

        keys = self.settings.classify_keys
        if not keys:
            keys = self.settings.gemini_keys  # fallback якщо окремих ключів немає
        self.llm = LLMClient(
            keys=keys,
            models=[cfg.model],
            gemma_only=False,
            service="DisciplineAssign",
        )
        # Ротація лише по одній моделі (Gemini 3.5 Flash Lite)
        self.llm._gemma_models = [cfg.model]
        self.llm._models = [cfg.model]
        self._running = True

    async def run(self) -> None:
        log = logger.bind(worker=f"discipline_assign-{self.worker_id}")
        cfg = self.settings.discipline_assign
        log.info(
            "discipline_assign_worker_started",
            llm_enabled=self.llm.enabled,
            keys=len(self.llm._keys),
            model=cfg.model,
        )

        try:
            await self.llm.initialize()
        except AllLimitsExhausted:
            sleep_s = (_tomorrow_midnight_utc() - datetime.now(UTC)).total_seconds()
            log.critical("discipline_assign_all_keys_exhausted_sleep", sleep_s=int(sleep_s))
            await asyncio.sleep(max(sleep_s, 60))
            return await self.run()

        db = build_database(self.settings)
        try:
            await db.initialize(sync_mirror=False)
        except Exception as e:  # noqa: BLE001
            log.error("discipline_assign_db_init_failed", error=str(e)[:200])
            return

        # Список дисциплін для присвоювання (одного разу за старт)
        try:
            from harvester.classify.taxonomy import load_disciplines

            disciplines = await load_disciplines(db)
        except Exception as e:  # noqa: BLE001
            log.error("discipline_assign_load_failed", error=str(e)[:200])
            await db.close()
            return
        if not disciplines:
            log.warning("discipline_assign_empty_list")
            await db.close()
            return

        disciplines_list = "\n".join(
            f"- {t['code']} — {t['name_uk']}" for t in disciplines
        )
        code_to_id = {t["code"]: t["id"] for t in disciplines}

        try:
            while self._running:
                try:
                    batch_size = cfg.batch_size
                    interval_s = cfg.interval_s
                    recheck_days = cfg.recheck_days
                    cutoff = (_utcnow() - timedelta(days=recheck_days)).isoformat()

                    rows = await db.fetchall(
                        """
                        SELECT d.* FROM documents d
                        WHERE d.status='verified'
                          AND (d.discipline_checked_at IS NULL OR d.discipline_checked_at < ?)
                        ORDER BY COALESCE(d.discipline_checked_at, '1970-01-01') ASC, d.verified_at DESC
                        LIMIT ?
                        """,
                        (cutoff, batch_size),
                    )

                    if not rows:
                        log.info("discipline_assign_batch_empty_sleep", interval_s=interval_s)
                        await asyncio.sleep(interval_s)
                        continue

                    log.info("discipline_assign_batch_start", count=len(rows))

                    for r in rows:
                        doc = dict(r)
                        doc_id = doc["id"]
                        log_doc = log.bind(doc_id=doc_id)

                        try:
                            picked, confidence = await self._assign(doc, disciplines_list)
                        except AllLimitsExhausted:
                            sleep_s = (_tomorrow_midnight_utc() - datetime.now(UTC)).total_seconds()
                            log.critical(
                                "discipline_assign_all_keys_exhausted_sleep",
                                sleep_s=int(sleep_s),
                                doc_id=doc_id,
                            )
                            await asyncio.sleep(max(sleep_s, 60))
                            self.llm._daily_limit_exhausted.clear()
                            self.llm._gemma_limit_exhausted.clear()
                            self.llm._initialized = False
                            break  # перервати батч, дочекатись сну, почати заново

                        await self._save_result(db, doc_id, picked, confidence, code_to_id, cfg.model)
                        log_doc.info(
                            "discipline_assign_done",
                            disciplines=picked,
                            confidence=round(confidence, 3),
                        )

                    await asyncio.sleep(interval_s)

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    log.exception("discipline_assign_worker_error", error=str(e))
                    await asyncio.sleep(10)
        finally:
            await db.close()
            log.info("discipline_assign_worker_stopped")

    async def _assign(self, doc: dict, disciplines_list: str) -> tuple[list[str], float]:
        """Викликати LLM та повернути (коди дисциплін, confidence)."""
        cfg = self.settings.discipline_assign
        text_sample = (doc.get("text_sample") or "")[: cfg.max_text_chars] or (
            doc.get("title") or ""
        )[:2000]

        prompt = DISCIPLINE_ASSIGN_PROMPT.format(
            title=doc.get("title") or doc.get("title_hint") or "невідомо",
            authors=doc.get("authors") or "невідомі",
            language=doc.get("language") or "невідома",
            udc=doc.get("udc") or "—",
            doc_type=doc.get("doc_type") or "article",
            text_sample=text_sample,
            disciplines_list=disciplines_list,
        )

        resp = await self.llm.complete(prompt)
        data = _extract_json(resp.text.strip())

        codes = [
            str(c).strip()
            for c in (data.get("disciplines") or [])
            if isinstance(c, str) and c.strip()
        ][: cfg.max_disciplines]
        try:
            confidence = float(data.get("confidence") or 0.0)
        except (TypeError, ValueError):
            confidence = 0.0
        confidence = max(0.0, min(1.0, confidence))

        if confidence < cfg.min_confidence:
            codes = []

        logger.info(
            "discipline_assign_llm",
            provider=resp.provider,
            model=resp.model,
            disciplines=codes,
            confidence=confidence,
        )
        return codes, confidence

    async def _save_result(
        self,
        db,
        doc_id: int,
        codes: list[str],
        confidence: float,
        code_to_id: dict[str, int],
        model: str,
    ) -> None:
        now = _utcnow().isoformat()
        score = max(confidence, 0.5)

        for code in codes:
            tid = code_to_id.get(code)
            if tid is None:
                logger.warning("discipline_assign_unknown_code", doc_id=doc_id, code=code)
                continue
            try:
                await db.execute(
                    """
                    INSERT OR IGNORE INTO document_topics (document_id, topic_id, score, signals)
                    VALUES (?, ?, ?, ?)
                    """,
                    (doc_id, tid, score, json.dumps({"discipline_assign": model}, ensure_ascii=False)),
                )
            except Exception as e:  # noqa: BLE001
                logger.warning("discipline_assign_insert_failed", doc_id=doc_id, topic_id=tid, error=str(e)[:100])

        await db.execute(
            "UPDATE documents SET discipline_checked_at = ? WHERE id = ?",
            (now, doc_id),
        )

    async def stop(self) -> None:
        self._running = False