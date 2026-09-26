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

from harvester.classify.llm import AllLimitsExhausted, LLMClient, log_raw_response
from harvester.config import Settings, get_settings
from harvester.db.connection import Database
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

    def __init__(
        self,
        worker_id: int = 0,
        db: Database | None = None,
        settings: Settings | None = None,
    ):
        self.worker_id = worker_id
        self.settings = settings or get_settings()
        self.db = db
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

        while self._running:
            try:
                await self.llm.initialize()
                break
            except AllLimitsExhausted:
                sleep_s = (_tomorrow_midnight_utc() - datetime.now(UTC)).total_seconds()
                log.critical("discipline_assign_all_keys_exhausted_sleep", sleep_s=int(sleep_s))
                await asyncio.sleep(max(sleep_s, 60))
                self.llm._daily_limit_exhausted.clear()
                self.llm._gemma_limit_exhausted.clear()
                self.llm._initialized = False

        if not self._running:
            return

        db = self.db
        owns_db = db is None
        if db is None:
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
            if owns_db:
                await db.close()
            return
        if not disciplines:
            log.warning("discipline_assign_empty_list")
            if owns_db:
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
                    stats = {"processed": 0, "assigned": 0, "no_disciplines": 0, "errors": 0}

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
                        except asyncio.CancelledError:
                            raise
                        except Exception as e:  # noqa: BLE001
                            # Ізоляція документа: раніше одна помилка розбору
                            # JSON (або збій запису) обривала весь батч —
                            # решту документів не обробляли до наступного
                            # циклу, і в логах лишався один рядок без doc_id.
                            stats["errors"] += 1
                            log_doc.exception(
                                "discipline_assign_error",
                                error=str(e)[:200],
                                error_type=type(e).__name__,
                            )
                            continue

                        try:
                            await self._save_result(
                                db, doc_id, picked, confidence, code_to_id, cfg.model
                            )
                        except Exception as e:  # noqa: BLE001
                            # Без discipline_checked_at документ наступного
                            # разу потрапить у той самий батч — без логу
                            # це виглядає як «воркер працює, а толку немає».
                            stats["errors"] += 1
                            log_doc.exception(
                                "discipline_assign_save_failed",
                                error=str(e)[:200],
                                error_type=type(e).__name__,
                            )
                            continue

                        stats["processed"] += 1
                        if picked:
                            stats["assigned"] += 1
                        else:
                            stats["no_disciplines"] += 1
                        log_doc.info(
                            "discipline_assign_done",
                            disciplines=picked,
                            confidence=round(confidence, 3),
                        )

                    log.info("discipline_assign_batch_done", **stats)

                    await asyncio.sleep(interval_s)

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    log.exception("discipline_assign_worker_error", error=str(e))
                    await asyncio.sleep(10)
        finally:
            if owns_db:
                try:
                    await db.close()
                except Exception as e:  # noqa: BLE001
                    log.warning("discipline_assign_db_close_failed", error=str(e)[:200])
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
        try:
            data = _extract_json(resp.text.strip())
        except (json.JSONDecodeError, ValueError) as e:
            # Сиру відповідь логуємо обов'язково: JSONDecodeError сам по
            # собі не відрізняє обрізаний вивід, thinking-бюджет і
            # HTML-помилку. Саме через відсутність цієї відповіді дефект
            # thinking-моделі був невидимим.
            log_raw_response(
                logger, "discipline_assign_no_json", resp, doc_id=doc.get("id")
            )
            raise
        if not isinstance(data, dict):
            logger.warning(
                "discipline_assign_not_dict",
                doc_id=doc.get("id"),
                model=resp.model,
                got_type=type(data).__name__,
            )
            data = {}

        codes = [
            str(c).strip()
            for c in (data.get("disciplines") or [])
            if isinstance(c, str) and c.strip()
        ][: cfg.max_disciplines]
        try:
            confidence = float(data.get("confidence") or 0.0)
        except (TypeError, ValueError):
            # Мовчазне зменшення confidence до 0 робить результат «порожнім»,
            # тобто модель фактично втратила дисципліни — це видно лише
            # у лічильнику тем, а не в логах.
            logger.warning(
                "discipline_assign_confidence_invalid",
                doc_id=doc.get("id"),
                value=repr(data.get("confidence"))[:80],
            )
            confidence = 0.0
        confidence = max(0.0, min(1.0, confidence))

        if confidence < cfg.min_confidence:
            if codes:
                logger.info(
                    "discipline_assign_below_threshold",
                    doc_id=doc.get("id"),
                    confidence=confidence,
                    min_confidence=cfg.min_confidence,
                    dropped=len(codes),
                )
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
