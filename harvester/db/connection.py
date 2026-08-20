import asyncio
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

import aiosqlite
import structlog

from harvester.config import Settings

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
    """Локальна SQLite-БД (aiosqlite, autocommit, WAL)."""

    backend_kind = "sqlite"

    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)
        self._writer: aiosqlite.Connection | None = None
        self._readers: list[aiosqlite.Connection] = []
        self._reader_lock = asyncio.Lock()
        self._is_initialized = False

    async def initialize(self) -> None:
        if self._initialized:
            return

        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # isolation_level=None → autocommit: кожен запис одразу на диску,
        # без цього aiosqlite/sqlite3 тримає неявну транзакцію і дані губляться
        # при закритті без commit.
        self._writer = await aiosqlite.connect(str(self.db_path), isolation_level=None)
        self._writer.row_factory = sqlite3.Row
        await self._apply_pragmas(self._writer)

        self._is_initialized = True
        logger.info("database_initialized", path=str(self.db_path))

    async def _apply_pragmas(self, conn: aiosqlite.Connection) -> None:
        pragmas = [
            "PRAGMA journal_mode=WAL",
            "PRAGMA synchronous=NORMAL",
            "PRAGMA foreign_keys=ON",
            "PRAGMA busy_timeout=5000",
            "PRAGMA temp_store=MEMORY",
            "PRAGMA mmap_size=268435456",
        ]
        for pragma in pragmas:
            await conn.execute(pragma)

    async def _get_reader(self) -> aiosqlite.Connection:
        async with self._reader_lock:
            if not self._readers:
                reader = await aiosqlite.connect(str(self.db_path))
                reader.row_factory = sqlite3.Row
                await self._apply_pragmas(reader)
                self._readers.append(reader)
            return self._readers[0]

    async def execute(self, sql: str, params: tuple | None = None) -> sqlite3.Cursor:
        if params:
            return await self._writer.execute(sql, params)
        return await self._writer.execute(sql)

    async def executemany(self, sql: str, params: list[tuple]) -> None:
        await self._writer.executemany(sql, params)

    async def executescript(self, sql: str) -> None:
        await self._writer.executescript(sql)

    async def fetchone(self, sql: str, params: tuple | None = None) -> sqlite3.Row | None:
        reader = await self._get_reader()
        if params:
            async with reader.execute(sql, params) as cursor:
                return await cursor.fetchone()
        async with reader.execute(sql) as cursor:
            return await cursor.fetchone()

    async def fetchall(self, sql: str, params: tuple | None = None) -> list[sqlite3.Row]:
        reader = await self._get_reader()
        if params:
            async with reader.execute(sql, params) as cursor:
                return await cursor.fetchall()
        async with reader.execute(sql) as cursor:
            return await cursor.fetchall()

    async def insert(self, sql: str, params: tuple | None = None) -> int:
        cursor = await self._writer.execute(sql, params or ())
        return cursor.lastrowid

    async def update(self, sql: str, params: tuple | None = None) -> int:
        cursor = await self._writer.execute(sql, params or ())
        return cursor.rowcount

    async def delete(self, sql: str, params: tuple | None = None) -> int:
        cursor = await self._writer.execute(sql, params or ())
        return cursor.rowcount

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[sqlite3.Cursor]:
        # isolation_level=None → autocommit: кожен вираз — окрема транзакція,
        # тож COMMIT/ROLLBACK не потрібні (і викликали 6 помилку без активної транзакції).
        try:
            yield self._writer
        except Exception:
            raise

    async def _version(self) -> int:
        row = await self.fetchone("PRAGMA user_version")
        return row[0] if row else 0

    def probe(self) -> bool:
        return self._writer is not None

    async def close(self) -> None:
        if self._writer:
            try:
                await self._writer.close()
            except Exception as e:
                logger.warning("error_closing_writer", error=str(e))
            self._writer = None

        for reader in self._readers:
            try:
                await reader.close()
            except Exception as e:
                logger.warning("error_closing_reader", error=str(e))
        self._readers.clear()

        self._is_initialized = False
        logger.info("database_closed")