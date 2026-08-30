"""VerifierWorker — 24/7 перевірка джерел за strict-правилами."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import structlog

from harvester.classify.llm import AllLimitsExhausted, LLMClient
from harvester.config import get_settings
from harvester.db.failover import build_database

logger = structlog.get_logger()


def _tomorrow_midnight_utc() -> datetime:
    now = datetime.now(timezone.utc)
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return tomorrow


class VerifierWorker:
    """Цикл перевірки verified-документів."""

    def __init__(self, worker_id: int = 0):
        self.worker_id = worker_id
        self.settings = get_settings()
        # Тільки GEMINI_DOC_VERIFIER_KEY_1..4 + Gemini 3.1 Flash Lite
        keys = self.settings.classify_keys
        if not keys:
            keys = self.settings.gemini_keys  # fallback якщо немає окремих
        self.llm = LLMClient(keys=keys, models=["gemini-3.1-flash-lite"], gemma_only=False)
        # Перевизначимо gemma_models щоб ротація йшла лише по одній моделі
        self.llm._gemma_models = ["gemini-3.1-flash-lite"]
        self.llm._models = ["gemini-3.1-flash-lite"]
        self._running = True

    async def run(self) -> None:
        log = logger.bind(worker=f"verifier-{self.worker_id}")
        log.info("verifier_worker_started", llm_enabled=self.llm.enabled, keys=len(self.llm._keys))

        # Чекаємо ініціалізації LLM
        try:
            await self.llm.initialize()
        except AllLimitsExhausted:
            sleep_s = ( _tomorrow_midnight_utc() - datetime.now(timezone.utc)).total_seconds()
            log.critical("verifier_all_keys_exhausted_sleep", sleep_s=int(sleep_s))
            await asyncio.sleep(max(sleep_s, 60))
            return await self.run()

        db = build_database(self.settings)
        await db.initialize(sync_mirror=False)

        try:
            while self._running:
                try:
                    # Беремо батч verified-документів, які давно не перевірялись
                    batch_size = getattr(self.settings.verifier, "batch_size", 20) if hasattr(self.settings, "verifier") else 20
                    interval_s = getattr(self.settings.verifier, "interval_s", 60) if hasattr(self.settings, "verifier") else 60
                    recheck_days = getattr(self.settings.verifier, "recheck_days", 7) if hasattr(self.settings, "verifier") else 7

                    cutoff = (datetime.utcnow() - timedelta(days=recheck_days)).isoformat()

                    rows = await db.fetchall(
                        """
                        SELECT d.* FROM documents d
                        LEFT JOIN verifier_results vr ON vr.document_id = d.id AND vr.profile='strict'
                        WHERE d.status='verified'
                          AND (vr.checked_at IS NULL OR vr.checked_at < ? OR d.verifier_checked_at IS NULL OR d.verifier_checked_at < ?)
                        ORDER BY COALESCE(vr.checked_at, d.verifier_checked_at, '1970-01-01') ASC, d.verified_at DESC
                        LIMIT ?
                        """,
                        (cutoff, cutoff, batch_size),
                    )

                    if not rows:
                        log.info("verifier_batch_empty_sleep", interval_s=interval_s)
                        await asyncio.sleep(interval_s)
                        continue

                    log.info("verifier_batch_start", count=len(rows))

                    for r in rows:
                        doc = dict(r)
                        doc_id = doc["id"]
                        log_doc = log.bind(doc_id=doc_id)
                        log_doc.info("verifier_document_check_start", title=(doc.get("title") or "")[:60])

                        # 1. Швидкі strict-правила (без LLM, без мережі)
                        from harvester.verifier.rules import check_strict_rules

                        passed, failed_rules, comment = check_strict_rules(doc)

                        # 2. RU/СРСР фільтр (дешево)
                        if passed:
                            from harvester.verify.langid import detect_language

                            text_sample = (doc.get("text_sample") or doc.get("title") or "")[:2000]
                            if text_sample:
                                lang = await detect_language(text_sample)
                                if lang.language == "ru" and lang.confidence >= 0.8:
                                    passed, failed_rules, comment = False, ["russian_language"], "російська мова"

                        # 3. PDF-якість (потребує завантаження — пропускаємо якщо немає URL, інакше легка перевірка)
                        # Для економії — покладаємось на вже збережені has_text_layer/page_count

                        # 4. LLM-верифікація (дорого) — тільки якщо пройшли 1-2
                        llm_verdict, llm_comment, llm_conf = "skip", "", 0.0
                        llm_model = "gemini-3.1-flash-lite"
                        llm_key_idx = self.llm._key_idx
                        if passed:
                            try:
                                from harvester.verifier.llm_verifier import verify_with_llm

                                llm_verdict, llm_comment, llm_conf = await verify_with_llm(doc, self.llm)
                                log_doc.info("verifier_llm_ok", verdict=llm_verdict, comment=llm_comment[:100])
                                if llm_verdict == "fail":
                                    passed = False
                                    failed_rules.append(f"llm:{llm_comment[:80]}")
                                    comment = llm_comment or "LLM: не відповідає критеріям цілісності"
                                elif llm_verdict == "error":
                                    # Не вважаємо помилку LLM за fail — залишаємо passed з попередженням
                                    log_doc.warning("verifier_llm_error_ignored", error=llm_comment[:100])
                            except AllLimitsExhausted:
                                # Всі 4 ключі вичерпані — спати до 00:00 UTC
                                sleep_s = (_tomorrow_midnight_utc() - datetime.now(timezone.utc)).total_seconds()
                                log.critical("verifier_all_keys_exhausted_sleep", sleep_s=int(sleep_s), doc_id=doc_id)
                                await asyncio.sleep(max(sleep_s, 60))
                                # Очистити exhausted сети щоб зранку почати заново
                                self.llm._daily_limit_exhausted.clear()
                                self.llm._gemma_limit_exhausted.clear()
                                self.llm._initialized = False
                                break  # перервати батч, почати заново після сну
                            except Exception as e:  # noqa: BLE001
                                log_doc.warning("verifier_llm_error", error=str(e)[:150])
                                llm_verdict, llm_comment = "error", str(e)[:200]

                        # Запис результату
                        status = "pass" if passed else "fail"
                        now = datetime.utcnow().isoformat()
                        next_check = (datetime.utcnow() + timedelta(days=7 if passed else 30)).isoformat()
                        rules_failed_json = json.dumps(failed_rules, ensure_ascii=False)

                        await db.execute(
                            """
                            INSERT INTO verifier_results (document_id, profile, status, comment, rules_failed, llm_status, llm_comment, llm_model, llm_key_idx, checked_at, next_check_at)
                            VALUES (?, 'strict', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(document_id, profile) DO UPDATE SET
                              status=excluded.status, comment=excluded.comment, rules_failed=excluded.rules_failed,
                              llm_status=excluded.llm_status, llm_comment=excluded.llm_comment, llm_model=excluded.llm_model,
                              llm_key_idx=excluded.llm_key_idx, checked_at=excluded.checked_at, next_check_at=excluded.next_check_at
                            """,
                            (doc_id, status, comment[:500], rules_failed_json, llm_verdict, llm_comment[:500], llm_model, llm_key_idx, now, next_check),
                        )
                        # Дзеркало для швидких фільтрів
                        await db.execute(
                            "UPDATE documents SET verifier_status=?, verifier_comment=?, verifier_checked_at=? WHERE id=?",
                            (status, comment[:500], now, doc_id),
                        )
                        log_doc.info("verifier_result_saved", status=status, comment=comment[:100], next_check_at=next_check)

                    await asyncio.sleep(interval_s)

                except asyncio.CancelledError:
                    break
                except Exception as e:  # noqa: BLE001
                    log.error("verifier_worker_error", error=str(e), exc_info=True)
                    await asyncio.sleep(10)
        finally:
            await db.close()
            log.info("verifier_worker_stopped")

    async def stop(self) -> None:
        self._running = False
