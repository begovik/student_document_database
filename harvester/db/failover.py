"""Failover-обгортка: віддалена PostgreSQL з локальним SQLite як резервом.

Поведінка (згідно з ТЗ):
- при старті перевіряється доступ до віддаленої БД; якщо недоступна —
  робота йде на локальній;
- якщо запит до віддаленої БД не проходить, робиться кілька спроб
  (`retries`), після чого — перемикання на локальну;
- фоновий restore-probe повертає роботу на віддалену БД, щойно вона
  знову доступна, попередньо зливаючи (replay) зміни з локальної через
  outbox-таблицю.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from typing import Any

import structlog

from harvester.config import DatabaseConfig, get_settings
from harvester.db.connection import Database, SqliteDatabase
from harvester.db.dialect import ID_TABLES, inject_id, insert_id_table

try:  # pragma: no cover - asyncpg опційний
    from asyncpg.exceptions import UniqueViolationError as _AsyncpgUniqueError  # type: ignore
except ImportError:
    _AsyncpgUniqueError = None  # type: ignore

logger = structlog.get_logger()

OUTBOX_TABLE = "failover_outbox"
OUTBOX_DDL = (
    "CREATE TABLE IF NOT EXISTS failover_outbox ("
    "rid INTEGER PRIMARY KEY AUTOINCREMENT,"
    "op TEXT NOT NULL,"
    "sql TEXT NOT NULL,"
    "params TEXT NOT NULL)"
)

# Джерело id для рядків, створених у local-режимі: 2e9+ ніколи не конфліктує
# з id, які встигла видати remote-БД (серійні колонки реально < 1e9),
# і replay у remote виконується з тими самими id (FK-цілісність).
LOCAL_ID_BASE = 2_000_000_000


class RemoteUnavailable(Exception):
    """Віддалена БД недоступна (після всіх спроб)."""


class FailoverDatabase(Database):
    backend_kind = "failover"

    def __init__(self, cfg: DatabaseConfig | None = None, password: str | None = None):
        self.cfg = cfg or get_settings().database
        self._password = password
        self.remote: Database | None = None
        self.local: SqliteDatabase | None = None
        self._mode = "local"
        self._switch_lock = asyncio.Lock()
        self._drain_lock = asyncio.Lock()
        self._restore_task: asyncio.Task | None = None
        self._is_initialized = False
        self._remote_ever_ok = False
        self.db_path = self.cfg.local_db_path

    # ------------------------------------------------------------------ setup
    async def initialize(self) -> None:
        if self._initialized:
            return

        local_path = self.cfg.local_db_path or get_settings().db_path
        self.local = SqliteDatabase(local_path)
        await self.local.initialize()

        from harvester.db.migrations import apply_migrations

        await apply_migrations(self.local)
        await self.local.execute(OUTBOX_DDL)

        remote_ok = False
        if self.cfg.remote_configured:
            from harvester.db.postgres import PostgresDatabase

            self.remote = PostgresDatabase(
                self.cfg, password=self._password or get_settings().pg_password
            )
            remote_ok = await self.remote.try_connect(timeout_s=self.cfg.connect_timeout_s)
            if remote_ok:
                try:
                    await self.remote.initialize()
                except Exception as e:
                    logger.error("pg_initialize_failed", error=str(e))
                    remote_ok = False

        self._remote_ever_ok = remote_ok

        if remote_ok:
            # Якщо попередній запуск завершився під час аварії — спершу злити outbox.
            try:
                drained = await self._drain_outbox()
                if drained:
                    logger.info("db_startup_outbox_drained", ops=drained)
                self._mode = "remote"
                logger.info("db_mode_remote", host=self.cfg.host or "")
            except Exception as e:
                logger.error("db_startup_merge_failed", error=str(e))
                self._mode = "local"
        else:
            self._mode = "local"
            logger.warning(
                "db_mode_local",
                reason="remote_unavailable",
                host=self.cfg.host or "",
            )

        if self._mode == "local":
            self._start_restore_loop()

        self._is_initialized = True

    def _start_restore_loop(self) -> None:
        if self.remote is None:
            return
        if self._restore_task is None or self._restore_task.done():
            self._restore_task = asyncio.create_task(self._restore_loop())

    async def _restore_loop(self) -> None:
        """Періодично пробує повернутись на віддалену БД."""
        while True:
            await asyncio.sleep(self.cfg.restore_probe_interval_s)
            if self._mode == "remote" or self.remote is None:
                return

            ok = await self.remote.try_connect(timeout_s=self.cfg.connect_timeout_s)
            if not ok:
                logger.info("db_restore_probe_failed")
                continue

            # Пул може бути неініціалізований (старт відбувся на local).
            if not self._remote_ever_ok:
                try:
                    await self.remote.initialize()
                    self._remote_ever_ok = True
                except Exception as e:
                    logger.error("pg_reinit_failed", error=str(e))
                    continue

            try:
                await self._drain_outbox()
                await self._try_finalize_switch()
            except Exception as e:
                logger.error("db_restore_merge_error", error=str(e))

    async def _try_finalize_switch(self) -> None:
        """Фінальний прохід злиття під локом, потім — перемикання на remote."""
        async with self._switch_lock:
            if self._mode != "local":
                return
            await self._drain_outbox()
            row = await self.local.fetchone(f"SELECT COUNT(*) AS c FROM {OUTBOX_TABLE}")
            if row and row["c"] == 0:
                self._mode = "remote"
                logger.info("db_failover_restored")

    # ----------------------------------------------------------------- routing
    async def execute(self, sql: str, params: tuple | None = None) -> Any:
        return await self._route("execute", sql, params)

    async def executemany(self, sql: str, params: list[tuple]) -> None:
        await self._route("executemany", sql, [list(p) for p in params])

    async def executescript(self, sql: str) -> None:
        # Схемні маніпуляції в outbox не пишуться (виконуються при ініціалізації).
        active = self._active()
        if active is not None:
            await active.executescript(sql)

    async def fetchone(self, sql: str, params: tuple | None = None):
        async with self._switch_lock:
            if self._is_local_only_sql(sql):
                return await self.local.fetchone(sql, params)
            if self._mode == "remote":
                try:
                    return await self._remote_retry("fetchone", sql, params)
                except RemoteUnavailable:
                    await self._downgrade()
            return await self.local.fetchone(sql, params)

    async def fetchall(self, sql: str, params: tuple | None = None) -> list[Any]:
        async with self._switch_lock:
            if self._is_local_only_sql(sql):
                return await self.local.fetchall(sql, params)
            if self._mode == "remote":
                try:
                    return await self._remote_retry("fetchall", sql, params)
                except RemoteUnavailable:
                    await self._downgrade()
            return await self.local.fetchall(sql, params)

    async def insert(self, sql: str, params: tuple | None = None) -> int | None:
        return await self._route("insert", sql, params)

    async def update(self, sql: str, params: tuple | None = None) -> int:
        return await self._route("update", sql, params)

    async def delete(self, sql: str, params: tuple | None = None) -> int:
        return await self._route("delete", sql, params)

    @asynccontextmanager
    async def transaction(self):
        active = self._active()
        if active is None:
            yield None
            return
        async with active.transaction() as conn:
            yield conn

    async def _route(self, op: str, sql: str, params: Any = None) -> Any:
        """Єдина точка входу для DML-операцій (під локом перемикання)."""
        async with self._switch_lock:
            if self.local is None:
                raise RuntimeError("FailoverDatabase не ініціалізовано")

            if self._is_local_only_sql(sql):
                active = self._active()
                return await self._exec_on(active, op, sql, params)

            if self._mode == "remote":
                try:
                    return await self._remote_retry(op, sql, params)
                except RemoteUnavailable:
                    await self._downgrade()

            # Local-mode (або щойно перемкнено після невдачі remote).
            sql_for_store = sql
            if op in ("execute", "insert"):
                tbl = insert_id_table(sql)
                if tbl is not None:
                    next_id = await self._next_reserved_id(tbl)
                    injected = inject_id(sql, next_id)
                    if injected is not None:
                        sql = injected
                        sql_for_store = injected

            result = await self._exec_on(self.local, op, sql, params)
            await self._outbox_append(op, sql_for_store, params)
            return result

    async def _next_reserved_id(self, table: str) -> int:
        """Наступний id у зарезервованому діапазоні (>= LOCAL_ID_BASE).

        Викликається лише з local-режиму (під `_switch_lock` — послідовно),
        тож MAX+1 без гонок.
        """
        row = await self.local.fetchone(
            f"SELECT COALESCE(MAX(id), {LOCAL_ID_BASE - 1}) + 1 AS nid FROM {table}"
        )
        return int(row["nid"]) if row else LOCAL_ID_BASE

    def _active(self) -> Database | None:
        if self._mode == "remote":
            return self.remote
        return self.local

    @staticmethod
    def _is_local_only_sql(sql: str) -> bool:
        head = sql.lstrip().upper()
        return head.startswith("PRAGMA") or head.startswith("VACUUM")

    async def _remote_retry(self, op: str, sql: str, params: Any = None) -> Any:
        last_err: Exception | None = None
        for attempt in range(1, self.cfg.retries + 1):
            try:
                return await self._exec_on(self.remote, op, sql, params)
            except Exception as e:
                last_err = e
                logger.warning(
                    "db_remote_retry",
                    attempt=attempt,
                    total=self.cfg.retries,
                    error=str(e)[:300],
                )
                if attempt < self.cfg.retries:
                    await asyncio.sleep(self.cfg.retry_delay_s)
        raise RemoteUnavailable(str(last_err)) from last_err

    async def _downgrade(self) -> None:
        if self._mode == "remote":
            self._mode = "local"
            logger.critical("db_failover_started", host=self.cfg.host or "")
            self._start_restore_loop()

    @staticmethod
    async def _exec_on(db: Database | None, op: str, sql: str, params: Any = None) -> Any:
        if op == "execute":
            return await db.execute(sql, params)
        if op == "executemany":
            await db.executemany(sql, params or [])
            return None
        if op == "insert":
            return await db.insert(sql, params)
        if op == "update":
            return await db.update(sql, params)
        if op == "delete":
            return await db.delete(sql, params)
        if op == "fetchone":
            return await db.fetchone(sql, params)
        if op == "fetchall":
            return await db.fetchall(sql, params)
        raise ValueError(f"unknown op: {op}")

    # ------------------------------------------------------------------ outbox
    async def _outbox_append(self, op: str, sql: str, params: Any = None) -> None:
        """Запис операції у local-чергу для майбутнього replay у remote."""
        if self.local is None:
            return
        params_json = json.dumps(params if params is not None else [], ensure_ascii=False)
        await self.local.execute(
            f"INSERT INTO {OUTBOX_TABLE} (op, sql, params) VALUES (?, ?, ?)",
            (op, sql, params_json),
        )

    async def _drain_outbox(self) -> int:
        """Виконує накопичені операції у remote (у порядку rid).

        Рядок, що спричиняє порушення унікальності (дублікат у remote),
        вважається вже застосованим і пропускається (лог + продовження).
        """
        if self.remote is None:
            return 0
        async with self._drain_lock:
            total = 0
            while True:
                rows = await self.local.fetchall(
                    f"SELECT rid, op, sql, params FROM {OUTBOX_TABLE} ORDER BY rid LIMIT 1000"
                )
                if not rows:
                    break
                for row in rows:
                    params = json.loads(row["params"])
                    try:
                        await self._exec_on(self.remote, row["op"], row["sql"], params)
                    except Exception as e:
                        if self._is_unique_violation(e):
                            logger.warning(
                                "db_outbox_dup_skipped", rid=row["rid"], error=str(e)[:200]
                            )
                        else:
                            raise
                    await self.local.execute(
                        f"DELETE FROM {OUTBOX_TABLE} WHERE rid = ?", (row["rid"],)
                    )
                    total += 1
            if total:
                logger.info("db_outbox_replayed", ops=total)
            return total

    @staticmethod
    def _is_unique_violation(e: Exception) -> bool:
        name = type(e).__name__
        if name in ("UniqueViolationError", "IntegrityError"):
            return True
        if _AsyncpgUniqueError is not None and isinstance(e, _AsyncpgUniqueError):
            return True
        return False

    # ------------------------------------------------------------------- state
    @property
    def mode(self) -> str:
        return self._mode

    async def pending_outbox_count(self) -> int:
        if self.local is None:
            return 0
        row = await self.local.fetchone(f"SELECT COUNT(*) AS c FROM {OUTBOX_TABLE}")
        return row["c"] if row else 0

    async def remote_healthy(self) -> bool:
        if self.remote is None:
            return False
        return await self.remote.try_connect(timeout_s=self.cfg.connect_timeout_s)

    async def get_version(self) -> int:
        if self._mode == "remote" and self.remote is not None:
            return await self.remote.get_version()
        if self.local is not None:
            return await self.local.get_version()
        return 0

    async def close(self) -> None:
        if self._restore_task is not None:
            self._restore_task.cancel()
            try:
                await self._restore_task
            except (asyncio.CancelledError, Exception):
                pass
            self._restore_task = None
        if self.remote is not None:
            try:
                await self.remote.close()
            except Exception as e:
                logger.warning("error_closing_remote", error=str(e))
            self.remote = None
        if self.local is not None:
            try:
                await self.local.close()
            except Exception as e:
                logger.warning("error_closing_local", error=str(e))
            self.local = None
        self._is_initialized = False
        logger.info("failover_db_closed", mode=self._mode)


def build_database(settings=None) -> FailoverDatabase:
    """Фабрика: повертає FailoverDatabase (remote або local-only за конфігом)."""
    if settings is None:
        settings = get_settings()
    cfg = settings.database
    if cfg.mode == "local" or not cfg.remote_configured:
        from harvester.config import DatabaseConfig

        cfg = DatabaseConfig(
            mode="local",
            host="",
            local_db_path=cfg.local_db_path,
        )
    return FailoverDatabase(cfg, password=settings.pg_password)