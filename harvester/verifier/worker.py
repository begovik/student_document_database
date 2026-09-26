"""VerifierWorker — 24/7 перевірка джерел за strict-правилами."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import structlog

from harvester.classify.llm import AllLimitsExhausted, LLMClient
from harvester.config import Settings, get_settings
from harvester.db.connection import Database
from harvester.db.failover import build_database
from harvester.verifier.llm_verifier import MIN_LLM_CONFIDENCE

logger = structlog.get_logger()


def _tomorrow_midnight_utc() -> datetime:
    now = datetime.now(UTC)
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return tomorrow


def _new_batch_stats() -> dict[str, int]:
    """Лічильники одного батчу verifier-а.

    Без них не видно реального виходу: лише окремі `verifier_result_saved`
    не дають зрозуміти, скільки документів пройшло, скільки впало через
    помилку LLM, а скільки батч просто не встиг обробити.
    """
    return {
        "processed": 0,
        "pass": 0,
        "fail": 0,
        "errors": 0,
        "llm_error_retry": 0,
        "quota_defer": 0,
    }


class VerifierWorker:
    """Цикл перевірки verified-документів."""

    def __init__(
        self,
        worker_id: int = 0,
        db: Database | None = None,
        settings: Settings | None = None,
    ):
        self.worker_id = worker_id
        self.settings = settings or get_settings()
        self.db = db
        # Тільки GEMINI_DOC_VERIFIER_KEY_1..4 + Gemini 3.1 Flash Lite
        keys = self.settings.classify_keys
        if not keys:
            keys = self.settings.gemini_keys  # fallback якщо немає окремих
        self.llm = LLMClient(keys=keys, models=["gemini-3.1-flash-lite"], gemma_only=False, service="Verifier")
        self._running = True
        # Лічильник повторних LLM-помилок per-document: помилка інфраструктури
        # не повинна назавжди перетворити документ на fail, але й не має
        # перебиратися безкінечно.
        self._llm_error_attempts: dict[int, int] = {}
        self._max_llm_error_attempts = 3

    async def run(self) -> None:
        log = logger.bind(worker=f"verifier-{self.worker_id}")
        log.info("verifier_worker_started", llm_enabled=self.llm.enabled, keys=len(self.llm._keys))

        # Чекаємо ініціалізації LLM
        while self._running:
            try:
                await self.llm.initialize()
                break
            except AllLimitsExhausted:
                sleep_s = (_tomorrow_midnight_utc() - datetime.now(UTC)).total_seconds()
                # Не блокуємо весь процес до опівночі: shared limiter і
                # bounded retry дозволяють автоматично підхопити квоту після
                # rollover без ручного перезапуску.
                sleep_s = min(max(sleep_s, 60), 300)
                log.warning("verifier_all_keys_exhausted_defer", sleep_s=int(sleep_s))
                await asyncio.sleep(sleep_s)
                self.llm.reset_exhausted_state()

        if not self._running:
            return

        db = self.db
        owns_db = db is None
        if db is None:
            db = build_database(self.settings)
            await db.initialize(sync_mirror=False)

        # Завантажити теми для тегів (25 тем)
        try:
            from harvester.classify.taxonomy import load_topics

            all_topics = await load_topics(db)
        except Exception as e:  # noqa: BLE001
            # Не мовчки: без списку тем LLM-теги не мають куди мапитись,
            # тому кожен документ втрачає тематичні зв'язки без жодного
            # запису в логах. Тут падіння помітне одразу.
            log.exception("verifier_topics_load_failed", error=str(e)[:200])
            all_topics = []

        try:
            while self._running:
                try:
                    # Беремо батч verified-документів, які давно не перевірялись
                    batch_size = getattr(self.settings.verifier, "batch_size", 20) if hasattr(self.settings, "verifier") else 20
                    interval_s = getattr(self.settings.verifier, "interval_s", 60) if hasattr(self.settings, "verifier") else 60
                    recheck_days = getattr(self.settings.verifier, "recheck_days", 7) if hasattr(self.settings, "verifier") else 7

                    cutoff = (datetime.utcnow() - timedelta(days=recheck_days)).isoformat()
                    now_iso = datetime.utcnow().isoformat()

                    # next_check_at — реальний розклад повторної перевірки.
                    # Раніше селектор ігнорував його й кожні recheck_days
                    # перевіряв увесь пул заново, витрачаючи денну квоту LLM на
                    # повтори замість нових документів.
                    # llm_status='error' — це запис без LLM-вердикту (стара
                    # fail-open логіка записала їх як pass). Такі документи
                    # пріоритетні: їх треба перевірити, щоб статус відповідав
                    # доказу якості.
                    rows = await db.fetchall(
                        """
                        SELECT d.* FROM documents d
                        LEFT JOIN verifier_results vr ON vr.document_id = d.id AND vr.profile='strict'
                        WHERE d.status='verified'
                          AND (
                              vr.checked_at IS NULL
                              OR vr.llm_status = 'error'
                              OR (vr.next_check_at IS NOT NULL AND vr.next_check_at <= ?)
                              OR (vr.next_check_at IS NULL AND vr.checked_at < ?)
                          )
                        ORDER BY (vr.checked_at IS NULL) DESC,
                                 COALESCE(vr.next_check_at, vr.checked_at, '1970-01-01') ASC,
                                 d.verified_at DESC
                        LIMIT ?
                        """,
                        (now_iso, cutoff, batch_size),
                    )

                    if not rows:
                        log.info("verifier_batch_empty_sleep", interval_s=interval_s)
                        await asyncio.sleep(interval_s)
                        continue

                    log.info("verifier_batch_start", count=len(rows))
                    stats = _new_batch_stats()

                    # Тримаємо bounded-розмір лічильника помилок: doc_id, яких
                    # немає в поточному батчі, більше не актуальні.
                    if len(self._llm_error_attempts) > 1000:
                        current_ids = {int(r["id"]) for r in rows}
                        self._llm_error_attempts = {
                            doc: attempts
                            for doc, attempts in self._llm_error_attempts.items()
                            if doc in current_ids
                        }

                    for r in rows:
                        doc = dict(r)
                        doc_id = doc["id"]
                        log_doc = log.bind(doc_id=doc_id)
                        log_doc.debug("verifier_document_check_start", title=(doc.get("title") or "")[:60])

                        try:
                            outcome = await self._process_document(
                                doc, all_topics, recheck_days, log_doc, db
                            )
                        except asyncio.CancelledError:
                            raise
                        except Exception as e:  # noqa: BLE001
                            # Ізоляція документа: раніше будь-який виняток
                            # (BiGINT/таймаут/помилка запису) переривав увесь
                            # батч і лишався одним рядком «verifier_worker_error»
                            # без doc_id — тоді не було видно, скільки
                            # документів залишилося неперевіреними.
                            stats["errors"] += 1
                            log_doc.exception(
                                "verifier_document_error",
                                error=str(e)[:200],
                                error_type=type(e).__name__,
                            )
                            continue

                        stats["processed"] += 1
                        if outcome == "quota_exhausted":
                            stats["quota_defer"] += 1
                            # Батч перервано: решта документів лишається
                            # неперевіреною до відновлення квоти.
                            log.warning(
                                "verifier_batch_interrupted",
                                processed=stats["processed"],
                                remaining=len(rows) - stats["processed"],
                                reason="llm_quota_exhausted",
                            )
                            break
                        if outcome == "skipped":
                            stats["llm_error_retry"] += 1
                            continue
                        if outcome == "pass":
                            stats["pass"] += 1
                        else:
                            stats["fail"] += 1

                    log.info(
                        "verifier_batch_done",
                        **stats,
                        remaining_in_error_retry=len(self._llm_error_attempts),
                    )
                    stats = _new_batch_stats()

                    await asyncio.sleep(interval_s)

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    log.error("verifier_worker_error", error=str(e), exc_info=True)
                    await asyncio.sleep(10)
        finally:
            if owns_db:
                try:
                    await db.close()
                except Exception as e:  # noqa: BLE001
                    # Падіння закриття не повинно приховувати факт зупинки.
                    log.warning("verifier_db_close_failed", error=str(e)[:200])
            log.info("verifier_worker_stopped")

    async def _process_document(
        self,
        doc: dict,
        all_topics: list[dict],
        recheck_days: int,
        log_doc,
        db,
    ) -> str:
        """Перевірити один документ. Повертає один із outcome-ів.

        ``pass`` / ``fail``       — результат записано у БД;
        ``skipped``               — тимчасова помилка LLM, результат навмисно
                                    не записуємо (bounded-повтор згодом);
        ``quota_exhausted``       — всі ключі вичерпані, батч треба перервати.
        """
        doc_id = doc["id"]

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
            elif not doc.get("text_sample"):
                # Немає зразка тексту → мовний фільтр не працював. Це
                # знижує якість пулу, тому фіксуємо явно, а не мовчки.
                log_doc.debug("verifier_no_text_sample_for_lang_check")

        # 3. PDF-якість (потребує завантаження — пропускаємо якщо немає URL, інакше легка перевірка)
        # Для економії — покладаємось на вже збережені has_text_layer/page_count

        # 4. LLM-верифікація (дорого) — тільки якщо пройшли 1-2
        llm_verdict, llm_comment, llm_conf = "skip", "", 0.0
        llm_extracted_title: str | None = None
        llm_extracted_authors: list[str] | None = None
        llm_tags: list[str] = []
        llm_doc_type: str = "other"
        llm_model = "gemini-3.1-flash-lite"
        llm_key_idx = self.llm._key_idx
        if passed:
            try:
                from harvester.verifier.llm_verifier import verify_with_llm

                (
                    llm_verdict,
                    llm_comment,
                    llm_conf,
                    llm_extracted_title,
                    llm_extracted_authors,
                    llm_tags,
                    llm_doc_type,
                ) = await verify_with_llm(doc, self.llm, all_topics)
                log_doc.info(
                    "verifier_llm_ok",
                    verdict=llm_verdict,
                    confidence=llm_conf,
                    comment=llm_comment[:100],
                    extracted_title=llm_extracted_title[:60] if llm_extracted_title else None,
                    extracted_authors=llm_extracted_authors,
                    tags=llm_tags,
                    doc_type=llm_doc_type,
                )
                if llm_verdict == "fail":
                    passed = False
                    failed_rules.append(f"llm:{llm_comment[:80]}")
                    comment = llm_comment or "LLM: не відповідає критеріям цілісності"
                    self._llm_error_attempts.pop(doc_id, None)
                elif llm_verdict == "error":
                    # Fail-closed: доки немає успішної LLM-відповіді,
                    # документ не отримує pass. Але помилку інфраструктури
                    # не записуємо як остаточний fail одразу — даємо
                    # bounded-повтори, і лише потім фіксуємо fail.
                    attempts = self._llm_error_attempts.get(doc_id, 0) + 1
                    self._llm_error_attempts[doc_id] = attempts
                    log_doc.warning(
                        "verifier_llm_error_retry",
                        error=(llm_comment or "")[:100],
                        attempt=attempts,
                        max_attempts=self._max_llm_error_attempts,
                    )
                    if attempts < self._max_llm_error_attempts:
                        return "skipped"
                    passed = False
                    failed_rules.append("llm:error_after_retries")
                    comment = (
                        f"LLM-верифікація не вдалася {attempts} разів: "
                        f"{llm_comment or 'невідома помилка'}"[:200]
                    )
                elif llm_verdict == "pass" and llm_conf < MIN_LLM_CONFIDENCE:
                    passed = False
                    failed_rules.append("llm:low_confidence")
                    comment = "LLM-верифікація не досягла достатньої впевненості"
                    self._llm_error_attempts.pop(doc_id, None)
                elif llm_doc_type in {"thesis", "dissertation"}:
                    passed = False
                    failed_rules.append("non_target_type")
                    comment = "дисертація або дипломна робота не є цільовим типом джерела"
                    self._llm_error_attempts.pop(doc_id, None)
                else:
                    self._llm_error_attempts.pop(doc_id, None)
            except AllLimitsExhausted:
                # Всі ключі/моделі вичерпані — відкладаємо
                # batch, але не позначаємо документ як fail.
                sleep_s = (_tomorrow_midnight_utc() - datetime.now(UTC)).total_seconds()
                sleep_s = min(max(sleep_s, 60), 300)
                log_doc.warning("verifier_all_keys_exhausted_defer", sleep_s=int(sleep_s))
                await asyncio.sleep(sleep_s)
                self.llm.reset_exhausted_state()
                return "quota_exhausted"
            except Exception as e:  # noqa: BLE001
                # verify_with_llm уже перетворює помилки у вердикт "error",
                # тому сюди доходять лише внутрішні збої (напр. БД). Тож
                # це справжній дефект, а не помилка моделі.
                log_doc.error(
                    "verifier_llm_call_failed",
                    error=str(e)[:200],
                    error_type=type(e).__name__,
                )
                llm_verdict, llm_comment = "error", str(e)[:200]

        # 4b. LLM-витяг назви/авторів/тегів/типу — порівняння та оновлення
        try:
            await self._maybe_update_title_authors(
                doc, llm_extracted_title, llm_extracted_authors, log_doc, db
            )
            await self._maybe_update_tags_and_type(
                doc, llm_tags, llm_doc_type, all_topics, log_doc, db
            )
        except Exception as e:  # noqa: BLE001
            log_doc.warning("verifier_metadata_update_failed", error=str(e)[:150])

        # Запис результату
        status = "pass" if passed else "fail"
        now = datetime.utcnow().isoformat()
        # Pass перевіряємо за recheck_days, fail — не частіше
        # ніж раз на 30 днів: невиправданий повторний LLM-дзвінок
        # для вже відхиленого документа витрачає квоту.
        next_check_days = recheck_days if passed else max(recheck_days, 30)
        next_check = (datetime.utcnow() + timedelta(days=next_check_days)).isoformat()
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
        log_doc.info(
            "verifier_result_saved",
            status=status,
            llm_status=llm_verdict,
            rules_failed=failed_rules[:3],
            next_check_at=next_check,
        )
        return status

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
                except Exception as e:  # noqa: BLE001
                    # authors у БД — не JSON. Fallback трактує весь рядок
                    # як одного автора, через що коректні авторизації
                    # видаляються під час оновлення. Логуємо, бо це
                    # помітно впливає на метадані каталогу.
                    log_doc.warning(
                        "verifier_authors_parse_failed",
                        raw=db_authors_raw[:120],
                        error=str(e)[:120],
                        error_type=type(e).__name__,
                    )
                    db_authors = [db_authors_raw.strip()] if db_authors_raw.strip() else []
            elif isinstance(db_authors_raw, list):
                db_authors = [str(x).strip() for x in db_authors_raw if str(x).strip()]

            # Визначити чи DB-автори є сміттям
            def _is_garbage_authors(authors: list[str]) -> bool:
                if not authors:
                    return True
                if len(authors) == 1 and authors[0] in ("USER", "1", "Unknown", "service", "", "Admin", "Lena"):
                    return True
                return all(_re.match(r"^[А-ЩЬьюЯ]{1,3}\.[А-ЩЬьюЯ]{1,3}\.*$", a) for a in authors)

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

    async def _maybe_update_tags_and_type(
        self,
        doc: dict,
        llm_tags: list[str],
        llm_doc_type: str,
        all_topics: list[dict],
        log_doc,
        db,
    ) -> None:
        """Додати теги (topics) та виправити тип документа.

        - Теги: 25 тем, один документ може мати 2+ тем. Додаємо відсутні, не видаляємо існуючі.
        - Тип: article/book/textbook/methodical/thesis/dissertation/report/preprint/other
        """
        doc_id = doc.get("id")

        # --- Теги ---
        if llm_tags:
            # Мапа code -> id
            code_to_id = {t["code"]: t["id"] for t in all_topics}
            # Існуючі теги документа
            existing_rows = await db.fetchall(
                "SELECT topic_id FROM document_topics WHERE document_id = ?", (doc_id,)
            )
            existing_ids = {r["topic_id"] for r in existing_rows}
            # Визначити нові
            to_add: list[int] = []
            for code in llm_tags[:3]:  # до 3 тегів
                tid = code_to_id.get(code)
                if tid and tid not in existing_ids:
                    to_add.append(tid)
            if to_add:
                for tid in to_add:
                    try:
                        await db.execute(
                            "INSERT OR IGNORE INTO document_topics (document_id, topic_id, score, signals) VALUES (?, ?, ?, ?)",
                            (doc_id, tid, 0.85, json.dumps({"verifier_llm": llm_doc_type}, ensure_ascii=False)),
                        )
                    except Exception as e:  # noqa: BLE001
                        log_doc.warning("verifier_tag_insert_failed", topic_id=tid, error=str(e)[:100])
                log_doc.info("verifier_tags_added", doc_id=doc_id, added=to_add, llm_tags=llm_tags)

        # --- Тип документа ---
        if llm_doc_type and llm_doc_type != "other":
            db_type = (doc.get("doc_type") or "other").lower()
            # Нормалізуємо: магістерська -> thesis, реферат -> report, etc. LLM вже повертає DOC_TYPES
            if llm_doc_type != db_type:
                # Якщо в БД "other" або "article" за замовчуванням з pipeline — дозволяємо перезапис
                # Якщо вже стоїть конкретний тип (thesis/book) — перезаписуємо лише якщо LLM впевнено (verdict pass)
                should_update = db_type in ("other", "article", "preprint", "") or llm_doc_type in ("thesis", "dissertation", "book", "textbook", "methodical")
                if should_update or db_type == "other":
                    await db.execute("UPDATE documents SET doc_type = ? WHERE id = ?", (llm_doc_type, doc_id))
                    log_doc.info("verifier_doc_type_updated", doc_id=doc_id, old_type=db_type, new_type=llm_doc_type)

    async def stop(self) -> None:
        self._running = False
