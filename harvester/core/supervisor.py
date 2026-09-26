import asyncio
import signal
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any

import structlog

from harvester.config import Settings
from harvester.core.events import EventLogger
from harvester.core.scheduler import Scheduler
from harvester.core.workers import ClassifyWorker, DiscoveryWorker, VerifyWorker
from harvester.db.connection import Database
from harvester.db.failover import build_database
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
        self._worker_objs: list[Any] = []
        self._heartbeat_task: asyncio.Task | None = None
        self._worker_restart_delay_s = 5.0

    async def start(self) -> None:
        logger.info("supervisor_starting")

        self.db = build_database(self.settings)
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
        from harvester.discovery.openalex import create_openalex_iterators
        from harvester.discovery.querygen import (
            seed_discipline_queries,
            seed_discipline_topics,
            seed_queries,
        )
        from harvester.net.blacklist import BlacklistService, seed_blacklist

        BlacklistService.get().set_db(self.db)

        # Кожен seed-крок ізольовано: раніше падіння одного (напр. парсингу
        # каталогу дисциплін) зупиняло весь bootstrap, сервіс не піднімався,
        # а єдиний слід — трасировка у journal від systemd.
        seeds = {
            "blacklist": seed_blacklist,
            "topics": seed_topics,
            "discipline_topics": seed_discipline_topics,
            "queries": seed_queries,
            "discipline_queries": seed_discipline_queries,
        }
        results: dict[str, int] = {}
        for name, fn in seeds.items():
            try:
                results[name] = await fn(self.db)
            except Exception as e:  # noqa: BLE001
                logger.error(
                    "bootstrap_seed_failed",
                    seed=name,
                    error=str(e)[:200],
                    error_type=type(e).__name__,
                )
                results[name] = -1

        n_blacklist = results["blacklist"]
        n_topics = results["topics"]
        n_discipline_topics = results["discipline_topics"]
        n_queries = results["queries"]
        n_discipline_queries = results["discipline_queries"]

        # На старих базах частина query-пулу могла залишитися retired після
        # тимчасових помилок пошуку. Реактивуємо лише bounded-порцію.
        from harvester.db.repositories import SearchQueriesRepository

        queries_repo = SearchQueriesRepository(self.db)
        reactivated_queries = await queries_repo.reactivate_retired(limit=250)

        pending_search = await self.scheduler.pending_count("search")
        if self.settings.channels.ddgs.enabled and pending_search == 0:
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
            else:
                # Активних запитів немає — discovery мовчки зупиниться.
                logger.warning("bootstrap_no_active_search_queries")

        pending_oai = await self.scheduler.pending_count("api_iter")
        if self.settings.channels.openalex.enabled and pending_oai == 0:
            for it in create_openalex_iterators():
                await self.scheduler.schedule_task("api_iter", it, priority=15)

        # Bounded recovery: документи, застряглі в 'verifying' після падіння
        # процесу, повертаються в чергу та отримують probe-задачу. Порція
        # обмежена — це не mass requeue.
        recovered_stuck = await self._recover_stuck_verifying(limit=50)

        logger.info(
            "bootstrap_done",
            topics_seeded=n_topics,
            discipline_topics_seeded=n_discipline_topics,
            queries_seeded=n_queries,
            discipline_queries_seeded=n_discipline_queries,
            reactivated_queries=reactivated_queries,
            blacklist_seeded=n_blacklist,
            pending_search=pending_search,
            pending_api_iter=pending_oai,
            recovered_stuck_verifying=recovered_stuck,
            # -1 означає, що відповідний seed-крок упав.
            seed_failures=[k for k, v in results.items() if v < 0],
        )

    async def _recover_stuck_verifying(self, limit: int = 25) -> int:
        """Повернути bounded-порцію документів зі стану 'verifying' у чергу.

        Документ лишається у 'verifying', якщо процес зупинився під час
        перевірки (restart, OOM, kill). Recovery виконується порціями при
        bootstrap і в heartbeat-циклі, тому застряглі документи повертаються
        без ручного втручання та без mass requeue.
        """
        if not self.db or not self.scheduler:
            return 0
        from harvester.db.repositories import DocumentsRepository

        try:
            docs = await DocumentsRepository(self.db).recover_stuck_verifying(limit=limit)
        except Exception as e:  # noqa: BLE001
            logger.warning("recover_stuck_verifying_failed", error=str(e)[:200])
            return 0

        scheduled = 0
        failed: list[int] = []
        for doc_id in docs:
            try:
                await self.scheduler.schedule_task("probe", {"document_id": doc_id}, priority=5)
                scheduled += 1
            except Exception as e:  # noqa: BLE001
                # Раніше помилка планування обривала цикл: решта документів
                # лишалась у 'verifying' без задачі й без запису в логах.
                failed.append(doc_id)
                logger.warning(
                    "recover_stuck_verifying_schedule_failed",
                    document_id=doc_id,
                    error=str(e)[:200],
                    error_type=type(e).__name__,
                )
        if docs:
            logger.info(
                "recovered_stuck_verifying",
                found=len(docs),
                scheduled=scheduled,
                schedule_failed=len(failed),
            )
        return scheduled

    async def _start_workers(self) -> None:
        w = self.settings.workers

        for i in range(w.discovery):
            worker = DiscoveryWorker(i, self.settings, self.db, self.scheduler)
            self._worker_objs.append(worker)
            self._workers.append(self._spawn(f"discovery-{i}", worker.run))

        for i in range(w.verify):
            worker = VerifyWorker(i, self.settings, self.db, self.scheduler)
            self._worker_objs.append(worker)
            self._workers.append(self._spawn(f"verify-{i}", worker.run))

        # Classify воркери: один ключ × одна модель = один воркер
        classify_keys = self.settings.classify_keys
        classify_models = self.settings.llm.gemma_models
        if classify_keys:
            worker_id = 0
            for key in classify_keys:
                for model in classify_models:
                    worker = ClassifyWorker(worker_id, self.settings, self.db, self.scheduler,
                                           classify_key=key, classify_model=model)
                    self._worker_objs.append(worker)
                    self._workers.append(self._spawn(f"classify-{worker_id}", worker.run))
                    worker_id += 1
            logger.info(
                "workers_started",
                discovery=w.discovery,
                verify=w.verify,
                classify=worker_id,
                classify_mode="gemma_per_key_model",
            )
        else:
            for i in range(w.classify):
                worker = ClassifyWorker(i, self.settings, self.db, self.scheduler)
                self._worker_objs.append(worker)
                self._workers.append(self._spawn(f"classify-{i}", worker.run))
            logger.info(
                "workers_started",
                discovery=w.discovery,
                verify=w.verify,
                classify=w.classify,
            )

        # Verifier воркери — 24/7 перевірка джерел за strict-правилами (Gemini 3.1 Flash Lite)
        if w.verifier > 0 and self.settings.verifier.enabled:
            try:
                from harvester.verifier.worker import VerifierWorker

                for i in range(w.verifier):
                    v_worker = VerifierWorker(i, db=self.db, settings=self.settings)
                    self._worker_objs.append(v_worker)
                    self._workers.append(self._spawn(f"verifier-{i}", v_worker.run))
                logger.info("verifier_workers_started", count=w.verifier)
            except Exception as e:  # noqa: BLE001
                logger.warning("verifier_worker_start_failed", error=str(e)[:200])

        # DisciplineAssign воркери — 24/7 присвоювання дисциплін каталогу (Gemini 3.5 Flash Lite)
        if w.discipline_assign > 0 and self.settings.discipline_assign.enabled:
            try:
                from harvester.classify.discipline_assigner import DisciplineAssigner

                for i in range(w.discipline_assign):
                    a_worker = DisciplineAssigner(i, db=self.db, settings=self.settings)
                    self._worker_objs.append(a_worker)
                    self._workers.append(self._spawn(f"discipline-assign-{i}", a_worker.run))
                logger.info("discipline_assign_workers_started", count=w.discipline_assign)
            except Exception as e:  # noqa: BLE001
                logger.warning("discipline_assign_worker_start_failed", error=str(e)[:200])

    def _spawn(
        self,
        name: str,
        runner: Callable[[], Awaitable[Any]] | Awaitable[Any],
    ) -> asyncio.Task:
        """Створити worker- із охоронцем і bounded-перезапуском.

        ``runner`` є factory-ом (``worker.run``), тому один і той самий worker
        можна безпечно підняти після transient-ініціалізації або падіння.
        Старий coroutine-аргумент лише підтримується для сумісності й не
        перезапускається, оскільки coroutine не можна виконати двічі.
        """

        async def guarded():
            factory = runner if callable(runner) else None
            failures = 0
            while self._running:
                try:
                    if factory is not None:
                        await factory()
                    else:
                        await runner  # type: ignore[misc]
                        return
                    if not self._running:
                        return
                    logger.warning("worker_returned", worker=name)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    failures += 1
                    logger.critical(
                        "worker_died",
                        worker=name,
                        error=str(e),
                        exc_info=True,
                    )
                    if self.event_logger is not None:
                        try:
                            await self.event_logger.error(
                                "supervisor", "worker_died",
                                {"worker": name, "error": str(e), "failures": failures},
                            )
                        except Exception as ev_err:  # noqa: BLE001
                            # Падіння самої фіксації події не повинно
                            # приховувати факт смерті воркера.
                            logger.warning(
                                "worker_died_event_failed",
                                worker=name,
                                error=str(ev_err)[:200],
                            )
                if not self._running:
                    return
                delay = min(self._worker_restart_delay_s * (2 ** min(failures, 4)), 60.0)
                await asyncio.sleep(delay)

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

        # Кожен воркер зупиняється в ізольованому try: раніше падіння
        # stop() одного воркера не давало зупинити решту, закрити БД і
        # вийти — процес лишався висів у shutdown, і systemd вбирав його
        # по таймауту, обриваючи незакриті транзакції.
        stop_errors = 0
        for w in self._worker_objs:
            try:
                await w.stop()
            except Exception as e:  # noqa: BLE001
                stop_errors += 1
                logger.warning(
                    "worker_stop_failed",
                    worker=type(w).__name__,
                    error=str(e)[:200],
                    error_type=type(e).__name__,
                )

        for worker in self._workers:
            worker.cancel()
        if self._workers:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._workers, return_exceptions=True),
                    timeout=25,
                )
            except asyncio.TimeoutError:
                # Воркер, який не завершився за 25 с, не буде зупинений —
                # це треба бачити, бо він тримає lease-и та з'єднання.
                logger.warning("workers_shutdown_timeout", pending=len(self._workers))

        if self.scheduler:
            await self.scheduler.stop()

        if self.db and self.db._initialized:
            try:
                await self.event_logger.info("supervisor", "service_stopped", {})
            except Exception as e:  # noqa: BLE001
                logger.warning("service_stopped_event_failed", error=str(e)[:200])
            try:
                await self.db.close()
            except Exception as e:  # noqa: BLE001
                logger.error("db_close_failed", error=str(e)[:200])

        logger.info("supervisor_stopped", worker_stop_errors=stop_errors)

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
                # Вичерпання LLM не повинно зупиняти discovery/verify: LLM-
                # воркери самі відкладають свої задачі до відновлення квоти.
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    async def _check_llm_exhausted(self) -> bool:
        """Залишено для сумісності з викликачами; сервіс не зупиняється через LLM."""
        return False

    async def _handle_signal(self, sig: signal.Signals) -> None:
        logger.info("signal_received", signal=sig.name)
        await self.stop()

    async def _write_heartbeat(self) -> None:
        if self.db and self.db._initialized:
            try:
                repo = SettingsRepository(self.db)
                payload: dict[str, Any] = {
                    "ts": datetime.utcnow().isoformat(),
                    "workers": len([t for t in self._workers if not t.done()]),
                    "workers_dead": len([t for t in self._workers if t.done()]),
                }
                # Глибини черг і розподіл документів: heartbeat із лише
                # `workers` не дозволяє відрізнити «все працює» від
                # «черга росте, конвеєр стоїть» — саме через це
                # застряглі в verifying документи не помічались.
                if self.scheduler is not None:
                    try:
                        payload["queues"] = await self.scheduler.queue_depths()
                    except Exception as e:  # noqa: BLE001
                        logger.warning("heartbeat_queues_failed", error=str(e)[:200])
                try:
                    payload["documents"] = await self._document_status_counts()
                except Exception as e:  # noqa: BLE001
                    logger.warning("heartbeat_docs_failed", error=str(e)[:200])
                if self.event_logger is not None:
                    payload["event_db_failures"] = self.event_logger.events_db_failures
                payload["db_backend"] = getattr(self.db, "backend_kind", "unknown")
                import json
                await repo.set("heartbeat", json.dumps(payload))
            except Exception as e:
                logger.warning("heartbeat_write_failed", error=str(e))

    async def _document_status_counts(self) -> dict[str, int]:
        """Розподіл документів за статусом verification (read-only, GROUP BY)."""
        rows = await self.db.fetchall(
            "SELECT verifier_status, COUNT(*) AS n FROM documents "
            "GROUP BY verifier_status ORDER BY n DESC LIMIT 12"
        )
        return {str(r["verifier_status"] or "unset"): int(r["n"]) for r in rows}

    async def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(30)
                await self._write_heartbeat()
                if self.scheduler:
                    await self.scheduler.recover_stale_tasks()
                # Невелика bounded-порція щокварталу: документи, які залишилися
                # в 'verifying' після аварійної зупинки процесу.
                await self._recover_stuck_verifying(limit=25)
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
