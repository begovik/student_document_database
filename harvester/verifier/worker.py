"""VerifierWorker — 24/7 перевірка джерел за strict-правилами."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import structlog

from harvester.classify.llm import AllLimitsExhausted, LLMClient
from harvester.config import get_settings
from harvester.db.failover import build_database

logger = structlog.get_logger()


def _tomorrow_midnight_utc() -> datetime:
    now = datetime.now(UTC)
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
            sleep_s = ( _tomorrow_midnight_utc() - datetime.now(UTC)).total_seconds()
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
                        llm_extracted_title: str | None = None
                        llm_extracted_authors: list[str] | None = None
                        llm_model = "gemini-3.1-flash-lite"
                        llm_key_idx = self.llm._key_idx
                        if passed:
                            try:
                                from harvester.verifier.llm_verifier import verify_with_llm

                                llm_verdict, llm_comment, llm_conf, llm_extracted_title, llm_extracted_authors = await verify_with_llm(doc, self.llm)
                                log_doc.info(
                                    "verifier_llm_ok",
                                    verdict=llm_verdict,
                                    comment=llm_comment[:100],
                                    extracted_title=llm_extracted_title[:60] if llm_extracted_title else None,
                                    extracted_authors=llm_extracted_authors,
                                )
                                if llm_verdict == "fail":
                                    passed = False
                                    failed_rules.append(f"llm:{llm_comment[:80]}")
                                    comment = llm_comment or "LLM: не відповідає критеріям цілісності"
                                elif llm_verdict == "error":
                                    # Не вважаємо помилку LLM за fail — залишаємо passed з попередженням
                                    log_doc.warning("verifier_llm_error_ignored", error=llm_comment[:100])
                            except AllLimitsExhausted:
                                # Всі 4 ключі вичерпані — спати до 00:00 UTC
                                sleep_s = (_tomorrow_midnight_utc() - datetime.now(UTC)).total_seconds()
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

                        # 4b. LLM-витяг назви/авторів — порівняння та оновлення (якщо LLM повернув)
                        # Логіка: якщо в БД немає або сміття — записати; якщо неспівпадіння — замінити/доповнити
                        try:
                            await self._maybe_update_title_authors(
                                doc, llm_extracted_title, llm_extracted_authors, log_doc, db
                            )
                        except Exception as e:  # noqa: BLE001
                            log_doc.warning("verifier_title_authors_update_failed", error=str(e)[:150])

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
                except Exception as e:
                    log.error("verifier_worker_error", error=str(e), exc_info=True)
                    await asyncio.sleep(10)
        finally:
            await db.close()
            log.info("verifier_worker_stopped")

    async def _maybe_update_title_authors(
        self,
        doc: dict,
        llm_title: str | None,
        llm_authors: list[str] | None,
        log_doc,
        db,
    ) -> None:
        """Порівняти LLM-витягнуті назву/авторів з БД і оновити якщо треба.

        - Якщо в БД немає/порожньо/сміття — записати LLM-значення
        - Якщо неспівпадіння — замінити (титул) або доповнити (автори)
        """
        import re as _re

        doc_id = doc.get("id")
        updates: dict[str, str] = {}

        # --- Назва ---
        if llm_title:
            llm_title_norm = _re.sub(r"\s+", " ", llm_title.strip())
            db_title = (doc.get("title") or "").strip()
            db_title_norm = _re.sub(r"\s+", " ", db_title)

            # Визначити чи DB-назва є сміттям
            title_is_garbage = (
                not db_title_norm
                or len(db_title_norm) < 10
                or "microsoft word" in db_title_norm.lower()
                or db_title_norm.lower() in ("unknown", "untitled", "без назви")
                or db_title_norm.lower().endswith((".pdf", ".doc", ".docx"))
                or len(_re.findall(r"[a-zA-Zа-яА-ЯіІєЇїЄєҐёЁ]{2,}", db_title_norm)) < 2
            )

            # Порівняння без регістру/пробілів
            titles_equal = db_title_norm.lower() == llm_title_norm.lower() if db_title_norm else False
            titles_similar = (
                llm_title_norm.lower() in db_title_norm.lower() or db_title_norm.lower() in llm_title_norm.lower()
            ) if db_title_norm else False

            should_update_title = False
            reason = ""
            if title_is_garbage:
                should_update_title = True
                reason = "в БД відсутня/ garbage — запис LLM-назви"
            elif not titles_equal and not titles_similar and 5 <= len(llm_title_norm) <= 500:
                # Явне неспівпадіння — заміняємо (LLM бачить титул з PDF)
                should_update_title = True
                reason = "неспівпадіння назв — заміна на LLM-версію"

            if should_update_title:
                updates["title"] = llm_title_norm
                log_doc.info(
                    "verifier_title_updated",
                    old_title=db_title_norm[:80],
                    new_title=llm_title_norm[:80],
                    reason=reason,
                )

        # --- Автори ---
        if llm_authors:
            # Парсимо авторів з БД
            db_authors_raw = doc.get("authors")
            db_authors: list[str] = []
            if isinstance(db_authors_raw, str):
                try:
                    import json as _j

                    parsed = _j.loads(db_authors_raw)
                    if isinstance(parsed, list):
                        db_authors = [str(x).strip() for x in parsed if str(x).strip()]
                    else:
                        db_authors = [db_authors_raw.strip()] if db_authors_raw.strip() else []
                except Exception:
                    db_authors = [db_authors_raw.strip()] if db_authors_raw.strip() else []
            elif isinstance(db_authors_raw, list):
                db_authors = [str(x).strip() for x in db_authors_raw if str(x).strip()]

            # Визначити чи DB-автори є сміттям
            def _is_garbage_authors(authors: list[str]) -> bool:
                if not authors:
                    return True
                if len(authors) == 1 and authors[0] in ("USER", "1", "Unknown", "service", "", "Admin", "Lena"):
                    return True
                if all(_re.match(r"^[А-ЩЬьюЯ]{1,3}\.[А-ЩЬьюЯ]{1,3}\.*$", a) for a in authors):
                    return True
                return False

            authors_is_garbage = _is_garbage_authors(db_authors)

            # Нормалізуємо для порівняння
            db_set = {a.lower().strip() for a in db_authors}
            llm_set = {a.lower().strip() for a in llm_authors if a.strip()}

            should_update_authors = False
            new_authors: list[str] = db_authors
            reason_a = ""

            if authors_is_garbage:
                should_update_authors = True
                new_authors = llm_authors
                reason_a = "в БД відсутні/ garbage — запис LLM-авторів"
            elif llm_set and llm_set != db_set:
                # Доповнення: об'єднуємо унікальних
                merged = db_authors.copy()
                for a in llm_authors:
                    if a.lower().strip() not in db_set:
                        merged.append(a)
                # Якщо є нові — оновлюємо (заміна+доповнення)
                if len(merged) != len(db_authors):
                    should_update_authors = True
                    new_authors = merged[:10]  # обмеження
                    reason_a = "неспівпадіння — доповнення списку авторів"

            if should_update_authors:
                updates["authors"] = json.dumps(new_authors, ensure_ascii=False)
                log_doc.info(
                    "verifier_authors_updated",
                    old_authors=db_authors,
                    new_authors=new_authors,
                    reason=reason_a,
                )

        # Виконати UPDATE якщо є зміни
        if updates:
            set_clause = ", ".join(f"{k} = ?" for k in updates)
            params = list(updates.values()) + [doc_id]
            await db.execute(f"UPDATE documents SET {set_clause} WHERE id = ?", tuple(params))
            log_doc.info("verifier_metadata_updated", doc_id=doc_id, fields=list(updates.keys()))

    async def stop(self) -> None:
        self._running = False
