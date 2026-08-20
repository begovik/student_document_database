import asyncio
import json
from datetime import datetime, timedelta

import structlog

from harvester.db.connection import Database
from harvester.db.repositories import TasksRepository

logger = structlog.get_logger()


class Scheduler:
    def __init__(self, db: Database):
        self.db = db
        self.tasks_repo = TasksRepository(db)
        self._running = False

    async def start(self) -> None:
        self._running = True
        logger.info("scheduler_started")

    async def stop(self) -> None:
        self._running = False
        logger.info("scheduler_stopped")

    async def pick_task(
        self, lease_duration_s: int = 300, task_types: list[str] | None = None
    ) -> dict | None:
        if not self._running:
            return None

        task = await self.tasks_repo.pick_next(lease_duration_s, task_types)
        if task:
            logger.debug("task_picked", task_id=task["id"], task_type=task["type"])
        return task

    async def complete_task(self, task_id: int) -> None:
        await self.tasks_repo.complete(task_id)
        logger.debug("task_completed", task_id=task_id)

    async def fail_task(self, task_id: int, delay_s: int = 0) -> None:
        task = await self.tasks_repo.db.fetchone("SELECT * FROM tasks WHERE id = ?", (task_id,))
        if task and task["attempts"] < task["max_attempts"]:
            await self.tasks_repo.return_to_pending(task_id, delay_s)
            logger.debug("task_returned_to_pending", task_id=task_id, delay_s=delay_s)
        else:
            await self.tasks_repo.fail(task_id)
            logger.warning("task_failed_permanently", task_id=task_id)

    async def recover_stale_tasks(self) -> int:
        count = await self.tasks_repo.recover_stale_tasks()
        if count > 0:
            logger.info("recovered_stale_tasks", count=count)
        return count

    async def schedule_task(
        self,
        task_type: str,
        payload: dict,
        priority: int = 10,
        run_after: str | None = None,
        max_attempts: int = 5,
    ) -> int | None:
        task_id = await self.tasks_repo.insert(
            task_type, payload, priority, run_after, max_attempts
        )
        if task_id:
            logger.debug("task_scheduled", task_id=task_id, task_type=task_type, priority=priority)
        return task_id

    async def get_stats(self) -> dict:
        return await self.tasks_repo.count_by_status()

    async def get_stats_by_type(self) -> dict:
        return await self.tasks_repo.count_by_type()

    async def pending_count(self, task_type: str) -> int:
        return await self.tasks_repo.count_pending_by_type(task_type)
