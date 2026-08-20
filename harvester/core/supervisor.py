import asyncio
import signal
from datetime import datetime
from typing import Any

import structlog

from harvester.config import Settings
from harvester.core.events import EventLogger
from harvester.core.scheduler import Scheduler
from harvester.core.workers import ClassifyWorker, DiscoveryWorker, VerifyWorker
from harvester.db.connection import Database
from harvester.db.migrations import ensure_schema
from harvester.db.repositories import SettingsRepository

logger = structlog.get_logger()


class Supervisor:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.db: Database | None = None
        self.scheduler: Scheduler | None = None
        self.event_logger: EventLogger | None = None
        self._running = False
        self._stopped = False
        self._workers: list[asyncio.Task] = []
        self._worker_objs: list = []
        self._heartbeat_task: asyncio.Task | None = None

    async def start(self) -> None:
        logger.info("supervisor_starting")

        self.db = Database(self.settings.db_path)
        await self.db.initialize()
        await ensure_schema(self.db)

        self.scheduler = Scheduler(self.db)
        self.event_logger = EventLogger(self.db)
        await self.scheduler.start()

        await self._bootstrap()

        self._running = True
        await self._write_heartbeat()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        await self._start_workers()

        await self.event_logger.info("supervisor", "service_started", {
            "workers": len(self._workers),
        })
        logger.info("supervisor_started", workers=len(self._workers))

    async def _bootstrap(self) -> None:
        """Початкове наповнення: теми, пошукові запити, OpenAlex-ітератори."""
        from harvester.classify.taxonomy import seed_topics
        from harvester.discovery.querygen import seed_queries
        from harvester.discovery.openalex import create_openalex_iterators
        from harvester.net.blacklist import BlacklistService, seed_blacklist

        BlacklistService.get().set_db(self.db)
        n_blacklist = await seed_blacklist(self.db)
        n_topics = await seed_topics(self.db)
        n_queries = await seed_queries(self.db)

        pending_search = await self.scheduler.pending_count("search")
        if pending_search == 0:
            from harvester.db.repositories import SearchQueriesRepository

            queries_repo = SearchQueriesRepository(self.db)
            query = await queries_repo.pick_lru()
            if query:
                await self.scheduler.schedule_task(
                    "search",
                    {
                        "query_id": query["id"],
                        "query_text": query["text"],
                        "region": query["region"],
                        "topic_hint": query.get("topic_hint"),
                    },
                    priority=30,
                )

        pending_oai = await self.scheduler.pending_count("api_iter")
        if pending_oai == 0:
            for it in create_openalex_iterators():
                await self.scheduler.schedule_task("api_iter", it, priority=15)

        logger.info(
            "bootstrap_done",
            topics_seeded=n_topics,
            queries_seeded=n_queries,
            blacklist_seeded=n_blacklist,
            pending_search=pending_search,
            pending_api_iter=pending_oai,
        )

    async def _start_workers(self) -> None:
        w = self.settings.workers

        for i in range(w.discovery):
            worker = DiscoveryWorker(i, self.settings, self.db, self.scheduler)
            self._worker_objs.append(worker)
            self._workers.append(self._spawn(f"discovery-{i}", worker.run()))

        for i in range(w.verify):
            worker = VerifyWorker(i, self.settings, self.db, self.scheduler)
            self._worker_objs.append(worker)
            self._workers.append(self._spawn(f"verify-{i}", worker.run()))

        for i in range(w.classify):
            worker = ClassifyWorker(i, self.settings, self.db, self.scheduler)
            self._worker_objs.append(worker)
            self._workers.append(self._spawn(f"classify-{i}", worker.run()))

        logger.info(
            "workers_started",
            discovery=w.discovery,
            verify=w.verify,
            classify=w.classify,
        )

    def _spawn(self, name: str, coro) -> asyncio.Task:
        """Воркер із охоронцем: фатальні помилки логуються, а не зникають мовчки."""

        async def guarded():
            try:
                await coro
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.critical("worker_died", worker=name, error=str(e), exc_info=True)
                try:
                    await self.event_logger.error("supervisor", "worker_died",
                                                  {"worker": name, "error": str(e)})
                except Exception:
                    pass

        return asyncio.create_task(guarded(), name=name)

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        logger.info("supervisor_stopping")
        self._running = False

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        for w in self._worker_objs:
            await w.stop()

        for worker in self._workers:
            worker.cancel()
        if self._workers:
            await asyncio.wait_for(
                asyncio.gather(*self._workers, return_exceptions=True),
                timeout=25,
            )

        if self.scheduler:
            await self.scheduler.stop()

        if self.db and self.db._initialized:
            await self.event_logger.info("supervisor", "service_stopped", {})
            await self.db.close()

        logger.info("supervisor_stopped")

    async def run_forever(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(
                sig, lambda s=sig: asyncio.create_task(self._handle_signal(s))
            )

        await self.start()

        try:
            while self._running:
                await asyncio.sleep(1)
                if await self._check_llm_exhausted():
                    logger.critical("llm_limits_exhausted_stopping")
                    break
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    async def _check_llm_exhausted(self) -> bool:
        """Перевіряє, чи всі LLM ліміти вичерпані."""
        from harvester.classify.llm import AllLimitsExhausted
        try:
            from harvester.classify.classifier import Classifier
            classifier = Classifier(self.db)
            if classifier.llm.enabled and classifier.llm._initialized:
                if len(classifier.llm._daily_limit_exhausted) >= len(classifier.llm._keys) * len(classifier.llm._models):
                    return True
        except Exception:
            pass
        return False

    async def _handle_signal(self, sig: signal.Signals) -> None:
        logger.info("signal_received", signal=sig.name)
        await self.stop()

    async def _write_heartbeat(self) -> None:
        if self.db and self.db._initialized:
            try:
                repo = SettingsRepository(self.db)
                payload = {
                    "ts": datetime.utcnow().isoformat(),
                    "workers": len([t for t in self._workers if not t.done()]),
                }
                import json
                await repo.set("heartbeat", json.dumps(payload))
            except Exception as e:
                logger.warning("heartbeat_write_failed", error=str(e))

    async def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(30)
                await self._write_heartbeat()
                if self.scheduler:
                    await self.scheduler.recover_stale_tasks()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("heartbeat_loop_error", error=str(e))
                await asyncio.sleep(5)

    @property
    def is_running(self) -> bool:
        return self._running

    async def get_status(self) -> dict[str, Any]:
        if not self.db:
            return {"status": "not_initialized"}
        repo = SettingsRepository(self.db)
        heartbeat = await repo.get("heartbeat")
        scheduler_stats = await self.scheduler.get_stats() if self.scheduler else {}
        return {
            "status": "running" if self._running else "stopped",
            "heartbeat": heartbeat,
            "tasks": scheduler_stats,
        }
