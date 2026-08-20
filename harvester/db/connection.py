import asyncio
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import aiosqlite
import structlog

from harvester.config import get_settings

logger = structlog.get_logger()


class Database:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._writer: aiosqlite.Connection | None = None
        self._readers: list[aiosqlite.Connection] = []
        self._reader_lock = asyncio.Lock()
        self._initialized = False

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

        self._initialized = True
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
    async def transaction(self):
        try:
            yield self._writer
            await self._writer.execute("COMMIT")
        except Exception:
            await self._writer.execute("ROLLBACK")
            raise

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

        self._initialized = False
        logger.info("database_closed")
