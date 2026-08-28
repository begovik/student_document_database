import asyncio
import json
from datetime import datetime, timedelta

import structlog

from harvester.classify.classifier import Classifier
from harvester.classify.llm import AllLimitsExhausted
from harvester.config import Settings
from harvester.core.events import EventLogger
from harvester.core.scheduler import Scheduler
from harvester.db.connection import Database
from harvester.db.repositories import (
    ChannelStatsRepository,
    DocumentsRepository,
    DocumentRefsRepository,
    SearchQueriesRepository,
)
from harvester.dedup.urlnorm import normalize_url
from harvester.discovery.ddgs_search import DDGSSearchChannel
from harvester.discovery.openalex import OpenAlexChannel
from harvester.net.guards import is_url_allowed
from harvester.verify.pipeline import VerifyPipeline

logger = structlog.get_logger()

LANG_PRIORITY = {"uk": 100, "en": 50}
DEFAULT_PRIORITY = 10

RETRYABLE_CODES = {"HTTP_ERROR", "TIMEOUT", "DOWNLOAD_ERROR", "ERROR", "PARSE_ERROR"}


def lang_to_priority(language: str | None) -> int:
    return LANG_PRIORITY.get(language or "", DEFAULT_PRIORITY)


class DiscoveryWorker:
    """Бере задачі search/api_iter з черги, знаходить кандидатів, реєструє документи."""

    def __init__(self, worker_id: int, settings: Settings, db: Database, scheduler: Scheduler):
        self.worker_id = worker_id
        self.settings = settings
        self.db = db
        self.scheduler = scheduler
        self.events = EventLogger(db)
        self.stats = ChannelStatsRepository(db)
        self.docs_repo = DocumentsRepository(db)
        self.refs_repo = DocumentRefsRepository(db)
        self.queries_repo = SearchQueriesRepository(db)
        self.channels = {
            "search": DDGSSearchChannel(),
            "api_iter": OpenAlexChannel(),
        }
        self._running = True

    async def run(self) -> None:
        log = logger.bind(worker=f"discovery-{self.worker_id}")
        log.info("discovery_worker_started")
        idle_streak = 0

        while self._running and self.scheduler._running:
            try:
                task = await self.scheduler.pick_task(task_types=["search", "api_iter"])
                if task is None:
                    idle_streak += 1
                    await self._ensure_search_task()
                    await asyncio.sleep(min(5 * idle_streak, 30))
                    continue

                idle_streak = 0
                await self._process_task(task)

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("discovery_worker_error", error=str(e), exc_info=True)
                await self.events.error("discovery", "worker_loop_error", {"error": str(e)})
                await asyncio.sleep(10)

        log.info("discovery_worker_stopped")

    async def stop(self) -> None:
        self._running = False

    async def _ensure_search_task(self) -> None:
        """Якщо немає активних search-задач — запланувати LRU-запит."""
        pending = await self.scheduler.pending_count("search")
        if pending > 0:
            return

        query = await self.queries_repo.pick_lru()
        if query is None:
            return

        await self.scheduler.schedule_task(
            "search",
            {
                "query_id": query["id"],
                "query_text": query["text"],
                "region": query["region"],
                "topic_hint": query.get("topic_hint"),
            },
            priority=20,
        )

    async def _process_task(self, task: dict) -> None:
        task_id = task["id"]
        task_type = task["type"]
        payload = json.loads(task["payload"])
        started = datetime.utcnow()

        channel = self.channels.get(task_type)
        if channel is None:
            await self.scheduler.fail_task(task_id)
            return

        log = logger.bind(task_id=task_id, task_type=task_type, worker=f"discovery-{self.worker_id}")
        log.info("discovery_task_start", payload=payload)

        new_count = 0
        found_count = 0

        try:
            async for candidate in channel.discover(payload):
                found_count += 1
                inserted = await self._register_candidate(candidate)
                if inserted:
                    new_count += 1

            await self.scheduler.complete_task(task_id)
            await self.stats.increment(
                channel.name, requests=1, ok=1, items_found=found_count, items_new=new_count
            )
            log.info("discovery_task_done", found=found_count, new=new_count,
                     duration_s=round((datetime.utcnow() - started).total_seconds(), 1))

            if task_type == "search" and payload.get("query_id"):
                await self.queries_repo.record_run(payload["query_id"], new_count)
                await self._schedule_next_search(payload)
            elif task_type == "api_iter":
                next_cursor = getattr(channel, "last_next_cursor", None)
                await self._schedule_next_openalex_page(payload, next_cursor)

            if hasattr(channel, "wait_interval"):
                await channel.wait_interval()

        except Exception as e:
            log.error("discovery_task_error", error=str(e), exc_info=True)
            await self.stats.increment(channel.name, requests=1, errors=1)
            await self.scheduler.fail_task(task_id, delay_s=300)
            await self.events.error("discovery", "task_failed", {"task_id": task_id, "error": str(e)})

    async def _schedule_next_search(self, payload: dict) -> None:
        """Одразу плануємо наступний LRU-запит, щоб discovery не зупинявся."""
        await self._ensure_search_task()

    async def _schedule_next_openalex_page(self, payload: dict, next_cursor: str | None) -> None:
        """OpenAlex курсорна пагінація: плануємо наступну сторінку або відкладений рестарт циклу."""
        if next_cursor:
            await self.scheduler.schedule_task(
                "api_iter",
                {"filters": payload.get("filters", {}), "cursor": next_cursor},
                priority=15,
            )
        else:
            restart_at = (datetime.utcnow() + timedelta(hours=24)).isoformat()
            await self.scheduler.schedule_task(
                "api_iter",
                {"filters": payload.get("filters", {}), "cursor": "*"},
                priority=5,
                run_after=restart_at,
            )

    async def _register_candidate(self, candidate) -> bool:
        canonical = normalize_url(candidate.url)
        allowed, reason = await is_url_allowed(canonical)
        if not allowed:
            return False

        doc_id = await self.docs_repo.insert_or_ignore(
            canonical_url=canonical,
            landing_url=candidate.landing_url,
            source_id=candidate.source_id,
            doi=candidate.doi,
            isbn=candidate.isbn,
            openalex_id=candidate.openalex_id,
            title=candidate.title,
            title_hint=candidate.title_hint,
            authors=candidate.authors,
            year=candidate.year,
            publisher=candidate.publisher,
            language=candidate.language,
            lang_confidence=candidate.lang_confidence,
            doc_type=candidate.doc_type,
            udc=candidate.udc,
            is_oa=candidate.is_oa,
            oa_status=candidate.oa_status,
            extra=candidate.extra,
        )

        if doc_id is None:
            return False

        await self.refs_repo.insert(
            document_id=doc_id,
            found_via=candidate.channel or "unknown",
            channel=candidate.channel,
            query_text=candidate.query_text,
            ref_url=candidate.ref_url,
        )

        priority = lang_to_priority(candidate.language)
        await self.scheduler.schedule_task(
            "probe", {"document_id": doc_id}, priority=priority
        )
        logger.debug("candidate_registered", doc_id=doc_id, url=canonical, priority=priority)
        return True


class VerifyWorker:
    """Бере probe-задачі, виконує повну верифікацію PDF."""

    def __init__(self, worker_id: int, settings: Settings, db: Database, scheduler: Scheduler):
        self.worker_id = worker_id
        self.settings = settings
        self.db = db
        self.scheduler = scheduler
        self.events = EventLogger(db)
        self.stats = ChannelStatsRepository(db)
        self.docs_repo = DocumentsRepository(db)
        self._running = True

    async def run(self) -> None:
        log = logger.bind(worker=f"verify-{self.worker_id}")
        log.info("verify_worker_started")
        idle_streak = 0

        from harvester.net.client import HttpClient

        try:
            http_client = await HttpClient.get_instance()
        except Exception as e:
            log.error("verify_worker_init_failed", error=str(e), exc_info=True)
            await self.events.error("verify", "http_client_init_failed", {"error": str(e)})
            return

        pipeline = VerifyPipeline(self.db, http_client)

        while self._running and self.scheduler._running:
            try:
                task = await self.scheduler.pick_task(
                    lease_duration_s=int(self.settings.http.total_timeout_s) + 120,
                    task_types=["probe"],
                )
                if task is None:
                    idle_streak += 1
                    await asyncio.sleep(min(3 * idle_streak, 30))
                    continue

                idle_streak = 0
                await self._process_task(task, pipeline)

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("verify_worker_error", error=str(e), exc_info=True)
                await self.events.error("verify", "worker_loop_error", {"error": str(e)})
                await asyncio.sleep(10)

        log.info("verify_worker_stopped")

    async def stop(self) -> None:
        self._running = False

    async def _process_task(self, task: dict, pipeline: VerifyPipeline) -> None:
        task_id = task["id"]
        payload = json.loads(task["payload"])
        doc_id = payload.get("document_id")
        log = logger.bind(task_id=task_id, doc_id=doc_id, worker=f"verify-{self.worker_id}")

        doc = await self.docs_repo.get_by_id(doc_id)
        if not doc:
            log.warning("verify_doc_not_found")
            await self.scheduler.complete_task(task_id)
            return

        if doc["status"] in ("verified", "filtered_ru", "filtered_soviet", "duplicate", "not_pdf"):
            log.debug("verify_already_done", status=doc["status"])
            await self.scheduler.complete_task(task_id)
            return

        log.info("verify_task_start", url=doc["canonical_url"])
        started = datetime.utcnow()

        await self.docs_repo.update_status(doc_id, "verifying")

        result = await pipeline.verify_document(
            doc_id, doc["canonical_url"], title_hint=doc.get("title_hint")
        )

        duration_s = round((datetime.utcnow() - started).total_seconds(), 1)

        if result.success:
            await self.scheduler.complete_task(task_id)
            await self.stats.increment("verify", requests=1, ok=1, items_new=1)
            log.info("verify_task_done", code=result.code, duration_s=duration_s)
            await self.scheduler.schedule_task(
                "classify", {"document_id": doc_id}, priority=5
            )
        else:
            attempts = doc["verify_attempts"] + 1
            await self.db.execute(
                "UPDATE documents SET verify_attempts = ? WHERE id = ?", (attempts, doc_id)
            )
            await self.stats.increment("verify", requests=1, errors=1)

            # Retry delays: 10 хв, 30 хв, потім — жодних ретраїв
            RETRY_DELAYS = [600, 1800]

            if result.code in RETRYABLE_CODES and attempts <= len(RETRY_DELAYS):
                delay = RETRY_DELAYS[attempts - 1]
                log.warning("verify_retry_scheduled", code=result.code, attempt=attempts, delay_s=delay)
                await self.docs_repo.update_status(doc_id, "queued")
                await self.scheduler.complete_task(task_id)
                retry_at = (datetime.utcnow() + timedelta(seconds=delay)).isoformat()
                await self.scheduler.schedule_task(
                    "probe", {"document_id": doc_id}, priority=5, run_after=retry_at
                )
            else:
                final_status = "broken" if result.code in RETRYABLE_CODES else result.code.lower()
                await self.docs_repo.update_status(doc_id, final_status)
                await self.scheduler.complete_task(task_id)
                log.warning("verify_task_failed", code=result.code, duration_s=duration_s)
                await self.events.error(
                    "verify", "document_verify_failed",
                    {"doc_id": doc_id, "url": doc["canonical_url"], "code": result.code,
                     "message": result.message},
                )


class ClassifyWorker:
    """Бере classify-задачі, класифікує документи (правила + УДК + LLM)."""

    def __init__(self, worker_id: int, settings: Settings, db: Database, scheduler: Scheduler,
                 classify_key: str | None = None, classify_model: str | None = None):
        self.worker_id = worker_id
        self.db = db
        self.scheduler = scheduler
        self.events = EventLogger(db)
        keys = [classify_key] if classify_key else None
        models = [classify_model] if classify_model else None
        self.classifier = Classifier(db, keys=keys, models=models, gemma_only=bool(classify_key))
        self._running = True

    async def run(self) -> None:
        log = logger.bind(worker=f"classify-{self.worker_id}")
        log.info("classify_worker_started", llm_enabled=self.classifier.llm.enabled)
        idle_streak = 0

        while self._running and self.scheduler._running:
            try:
                task = await self.scheduler.pick_task(lease_duration_s=180, task_types=["classify"])
                if task is None:
                    idle_streak += 1
                    await asyncio.sleep(min(5 * idle_streak, 30))
                    continue

                idle_streak = 0
                await self._process_task(task)

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("classify_worker_error", error=str(e), exc_info=True)
                await self.events.error("classify", "worker_loop_error", {"error": str(e)})
                await asyncio.sleep(10)

        log.info("classify_worker_stopped")

    async def stop(self) -> None:
        self._running = False

    async def _process_task(self, task: dict) -> None:
        task_id = task["id"]
        payload = json.loads(task["payload"])
        doc_id = payload.get("document_id")

        doc = await self.db.fetchone(
            "SELECT * FROM documents WHERE id = ?", (doc_id,)
        )
        if not doc or doc["status"] != "verified":
            await self.scheduler.complete_task(task_id)
            return

        doc_dict = dict(doc)
        try:
            result = await self.classifier.classify_document(doc_dict)
            await self.classifier.save_classification(doc_id, result)
        except AllLimitsExhausted as e:
            # Якщо помилка transient (500/503) — не зупиняємо воркер
            error_str = str(e)
            is_transient = any(code in error_str for code in ["500", "502", "503", "504"])
            if is_transient:
                logger.warning("classify_transient_error", worker=f"classify-{self.worker_id}",
                             error_msg=error_str[:200])
                await asyncio.sleep(10)
                await self.scheduler.complete_task(task_id)
                return
            logger.critical("classify_worker_all_limits_exhausted", worker=f"classify-{self.worker_id}")
            await self.events.error("classify", "all_limits_exhausted", {"worker": f"classify-{self.worker_id}"})
            self._running = False
            await self.scheduler.complete_task(task_id)
            return

        await self.scheduler.complete_task(task_id)
        logger.info(
            "classify_task_done",
            task_id=task_id,
            doc_id=doc_id,
            topics=[t[0] for t in result["topics"]],
            signals=result["signals"],
        )
