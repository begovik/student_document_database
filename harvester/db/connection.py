import asyncio
import sqlite3
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger()


class Database:
    """Спільний інтерфейс роботи з БД (використовується репозиторіями).

    Реалізації: `SqliteDatabase` (локальна), `PostgresDatabase` (віддалена),
    `FailoverDatabase` (автоматичний вибір між ними).
    """

    backend_kind: str = "base"
    db_path: Path | str | None = None

    async def initialize(self) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

    async def execute(self, sql: str, params: tuple | None = None) -> Any:
        raise NotImplementedError

    async def executemany(self, sql: str, params: list[tuple]) -> None:
        raise NotImplementedError

    async def executescript(self, sql: str) -> None:
        raise NotImplementedError

    async def fetchone(self, sql: str, params: tuple | None = None):
        raise NotImplementedError

    async def fetchall(self, sql: str, params: tuple | None = None) -> list[Any]:
        raise NotImplementedError

    async def insert(self, sql: str, params: tuple | None = None) -> int | None:
        raise NotImplementedError

    async def update(self, sql: str, params: tuple | None = None) -> int:
        raise NotImplementedError

    async def delete(self, sql: str, params: tuple | None = None) -> int:
        raise NotImplementedError

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Any]:
        raise NotImplementedError

    @property
    def _initialized(self) -> bool:
        return getattr(self, "_is_initialized", False)

    @_initialized.setter
    def _initialized(self, value: bool) -> None:
        self._is_initialized = value

    async def get_version(self) -> int:
        """Поточна версія схеми (для CLI doctor)."""
        return await self._version()

    async def _version(self) -> int:
        raise NotImplementedError


class SqliteDatabase(Database):
    """Локальна SQLite-БД (sqlite3, WAL).

    Операції виконуються під одним write-lock, а `transaction()` створює
    справжню транзакцію. Це важливо для select-then-insert та пакетної
    обробки, які інакше залишають частково записані дані після помилки.
    """

    backend_kind = "sqlite"

    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)
        self._conn: sqlite3.Connection | None = None
        self._write_lock = asyncio.Lock()
        self._transaction_owner: asyncio.Task | None = None
        self._is_initialized = False

    async def initialize(self, read_only: bool = False) -> None:
        if self._initialized:
            return

        if not read_only:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # isolation_level=None → autocommit: кожен запис одразу на диску,
        # без цього sqlite3 тримає неявну транзакцію і дані губляться при
        # закритті без commit.
        if read_only:
            uri = f"file:{self.db_path}?mode=ro"
            self._conn = sqlite3.connect(
                uri,
                uri=True,
                isolation_level=None,
                check_same_thread=False,
            )
        else:
            self._conn = sqlite3.connect(
                str(self.db_path),
                isolation_level=None,
                check_same_thread=False,
            )
        self._conn.row_factory = sqlite3.Row
        self._apply_pragmas_sync(self._conn, read_only=read_only)

        self._is_initialized = True
        logger.info("database_initialized", path=str(self.db_path))

    def _apply_pragmas_sync(self, conn: sqlite3.Connection, read_only: bool = False) -> None:
        pragmas = [
            "PRAGMA synchronous=NORMAL",
            "PRAGMA foreign_keys=ON",
            "PRAGMA busy_timeout=5000",
            "PRAGMA temp_store=MEMORY",
            "PRAGMA mmap_size=268435456",
        ]
        if not read_only:
            pragmas.insert(0, "PRAGMA journal_mode=WAL")
        for pragma in pragmas:
            conn.execute(pragma)

    async def execute(self, sql: str, params: tuple | None = None) -> sqlite3.Cursor:
        async with self._write_context() as conn:
            return self._execute_sync(conn, sql, params)

    async def executemany(self, sql: str, params: list[tuple]) -> None:
        async with self._write_context() as conn:
            self._executemany_sync(conn, sql, params)

    async def executescript(self, sql: str) -> None:
        async with self._write_context() as conn:
            self._executescript_sync(conn, sql)

    def _owns_transaction(self) -> bool:
        return self._transaction_owner is asyncio.current_task()

    @asynccontextmanager
    async def _write_context(self):
        if self._conn is None:
            raise RuntimeError("SqliteDatabase не ініціалізовано")
        if self._owns_transaction():
            yield self._conn
            return
        async with self._write_lock:
            yield self._conn

    @staticmethod
    def _execute_sync(
        conn: sqlite3.Connection,
        sql: str,
        params: tuple | None = None,
    ) -> sqlite3.Cursor:
        if params is not None:
            return conn.execute(sql, params)
        return conn.execute(sql)

    @staticmethod
    def _executemany_sync(
        conn: sqlite3.Connection,
        sql: str,
        params: list[tuple],
    ) -> None:
        conn.executemany(sql, params)

    @staticmethod
    def _executescript_sync(conn: sqlite3.Connection, sql: str) -> None:
        conn.executescript(sql)

    async def fetchone(self, sql: str, params: tuple | None = None) -> sqlite3.Row | None:
        async with self._write_context() as conn:
            return self._fetchone_sync(conn, sql, params)

    async def fetchall(self, sql: str, params: tuple | None = None) -> list[sqlite3.Row]:
        async with self._write_context() as conn:
            return self._fetchall_sync(conn, sql, params)

    @staticmethod
    def _fetchone_sync(
        conn: sqlite3.Connection,
        sql: str,
        params: tuple | None = None,
    ) -> sqlite3.Row | None:
        if params is not None:
            cursor = conn.execute(sql, params)
        else:
            cursor = conn.execute(sql)
        return cursor.fetchone()

    @staticmethod
    def _fetchall_sync(
        conn: sqlite3.Connection,
        sql: str,
        params: tuple | None = None,
    ) -> list[sqlite3.Row]:
        if params is not None:
            cursor = conn.execute(sql, params)
        else:
            cursor = conn.execute(sql)
        return cursor.fetchall()

    async def insert(self, sql: str, params: tuple | None = None) -> int:
        async with self._write_context() as conn:
            cursor = self._execute_sync(conn, sql, params or ())
            return cursor.lastrowid

    async def update(self, sql: str, params: tuple | None = None) -> int:
        async with self._write_context() as conn:
            cursor = self._execute_sync(conn, sql, params or ())
            return cursor.rowcount

    async def delete(self, sql: str, params: tuple | None = None) -> int:
        async with self._write_context() as conn:
            cursor = self._execute_sync(conn, sql, params or ())
            return cursor.rowcount

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[sqlite3.Cursor]:
        if self._conn is None:
            raise RuntimeError("SqliteDatabase не ініціалізовано")
        if self._owns_transaction():
            # Репозиторії можуть вкладати транзакційні контексти.
            yield self._conn
            return

        await self._write_lock.acquire()
        self._transaction_owner = asyncio.current_task()
        try:
            self._execute_sync(self._conn, "BEGIN IMMEDIATE")
            try:
                yield self._conn
            except BaseException:
                self._execute_sync(self._conn, "ROLLBACK")
                raise
            else:
                self._execute_sync(self._conn, "COMMIT")
        finally:
            self._transaction_owner = None
            self._write_lock.release()

    async def _version(self) -> int:
        row = await self.fetchone("PRAGMA user_version")
        return row[0] if row else 0

    def probe(self) -> bool:
        return self._conn is not None

    async def close(self) -> None:
        if self._conn:
            try:
                self._conn.close()
            except Exception as e:
                logger.warning("error_closing_writer", error=str(e))
            self._conn = None

        self._transaction_owner = None
        self._is_initialized = False
        logger.info("database_closed")
