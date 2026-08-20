"""Віддалений PostgreSQL-бекенд (asyncpg).

Імплементує інтерфейс `Database` для PostgreSQL: пул з'єднань,
автокоміт через autocommit-пул, трансляція діалекту через `harvester.db.dialect`,
та застосування PG-схеми/міграцій.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import structlog

try:
    import asyncpg  # type: ignore
    from asyncpg.pool import Pool  # type: ignore
except ImportError:  # pragma: no cover
    asyncpg = None  # type: ignore
    Pool = None  # type: ignore

from harvester.config import DatabaseConfig, get_settings
from harvester.db.connection import Database
from harvester.db.dialect import (
    crowcount_from_status,
    prepare,
    prepare_many,
    split_statements,
    translate_sql,
)

logger = structlog.get_logger()

PG_SCHEMA_PATH = Path(__file__).parent / "pg_schema.sql"
PG_MIGRATIONS_DIR = Path(__file__).parent / "pg_migrations"


class PGResult:
    """Адаптер результату execute для сумісності з sqlite3.Cursor."""

    __slots__ = ("rows", "rowcount", "lastrowid")

    def __init__(self, rows: list | None = None, rowcount: int = 0):
        self.rows = rows if rows is not None else []
        self.rowcount = len(self.rows) if rows is not None else rowcount
        first = self.rows[0] if self.rows else None
        self.lastrowid = dict(first).get("id") if first else None


class PostgresDatabase(Database):
    backend_kind = "postgres"

    def __init__(self, cfg: DatabaseConfig, password: str | None = None):
        self.cfg = cfg
        self._password = password if password is not None else get_settings().pg_password
        self._pool: Pool | None = None
        self._tx_conn: Any | None = None
        self._is_initialized = False
        self.db_path = f"postgresql://{self.cfg.user}@{self.cfg.host}:{self.cfg.port}/{self.cfg.name}"

    def _check_asyncpg(self) -> None:
        if asyncpg is None:
            raise RuntimeError(
                "asyncpg не встановлено. Виконайте: pip install 'asyncpg>=0.29.0'"
            )

    def _dsn(self) -> str:
        if self.cfg.dsn:
            return self.cfg.dsn
        user = self.cfg.user or "postgres"
        host = self.cfg.host
        port = self.cfg.port
        name = self.cfg.name
        if self._password:
            return f"postgresql://{user}:{self._password}@{host}:{port}/{name}"
        return f"postgresql://{user}@{host}:{port}/{name}"

    async def try_connect(self, timeout_s: float | None = None) -> bool:
        """Швидкий перевірковий `SELECT 1` одним з'єднанням."""
        self._check_asyncpg()
        if not self.cfg.remote_configured:
            return False
        conn = None
        try:
            conn = await asyncio.wait_for(
                asyncpg.connect(self._dsn(), timeout=self.cfg.connect_timeout_s),
                timeout=timeout_s or self.cfg.connect_timeout_s + 1,
            )
            await asyncio.wait_for(conn.fetchval("SELECT 1"), timeout=5)
            return True
        except Exception as e:
            logger.warning("pg_probe_failed", error=str(e))
            return False
        finally:
            if conn is not None:
                try:
                    await conn.close()
                except Exception:
                    pass

    async def initialize(self) -> None:
        if self._initialized:
            return
        self._check_asyncpg()

        self._pool = await asyncpg.create_pool(
            self._dsn(),
            min_size=self.cfg.pool_min_size,
            max_size=self.cfg.pool_max_size,
            timeout=self.cfg.connect_timeout_s,
            command_timeout=60.0,
        )
        await self._ensure_version_table()
        await self.apply_schema()
        self._is_initialized = True
        logger.info("pg_initialized", dsn_host=self.cfg.host, db=self.cfg.name)

    async def _ensure_version_table(self) -> None:
        await self._pool.execute(
            "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)"
        )

    async def apply_schema(self) -> None:
        current = await self._version()
        if current == 0 and PG_SCHEMA_PATH.exists():
            sql = PG_SCHEMA_PATH.read_text(encoding="utf-8")
            for stmt in split_statements(sql):
                await self._pool.execute(translate_sql(stmt))
            await self._pool.execute("INSERT INTO schema_version (version) VALUES (1)")
            current = 1
            logger.info("pg_schema_applied", version=1)

        if PG_MIGRATIONS_DIR.exists():
            for mf in sorted(PG_MIGRATIONS_DIR.glob("*.sql")):
                try:
                    version = int(mf.stem.split("_")[0])
                except (ValueError, IndexError):
                    continue
                if version > current:
                    sql = mf.read_text(encoding="utf-8")
                    for stmt in split_statements(sql):
                        await self._pool.execute(translate_sql(stmt))
                    await self._pool.execute(
                        "INSERT INTO schema_version (version) VALUES ($1)", version
                    )
                    current = version
                    logger.info("pg_migration_applied", file=mf.name, version=version)

    async def probe(self, timeout_s: float | None = None) -> bool:
        if self._pool is None:
            return False
        try:
            async with self._pool.acquire() as conn:
                await asyncio.wait_for(conn.fetchval("SELECT 1"), timeout=timeout_s or 5)
            return True
        except Exception:
            return False

    def _conn_or_pool(self):
        return self._tx_conn if self._tx_conn is not None else self._pool

    async def execute(self, sql: str, params: tuple | None = None) -> PGResult:
        if self._pool is None:
            raise RuntimeError("PostgresDatabase не ініціалізовано")
        pg_sql, mode = prepare(sql)
        params = list(params) if params else []
        target = self._conn_or_pool()

        if mode == "rows":
            rows = await target.fetch(pg_sql, *params)
            return PGResult(rows=rows)
        status = await target.execute(pg_sql, *params)
        return PGResult(rowcount=crowcount_from_status(status))

    async def executemany(self, sql: str, params: list[tuple]) -> None:
        if self._pool is None:
            raise RuntimeError("PostgresDatabase не ініціалізовано")
        pg_sql = prepare_many(sql)
        await self._pool.executemany(pg_sql, [list(p) for p in params])

    async def executescript(self, sql: str) -> None:
        if self._pool is None:
            raise RuntimeError("PostgresDatabase не ініціалізовано")
        self._check_asyncpg()
        for stmt in split_statements(sql):
            await self._pool.execute(translate_sql(stmt))

    async def fetchone(self, sql: str, params: tuple | None = None):
        if self._pool is None:
            raise RuntimeError("PostgresDatabase не ініціалізовано")
        pg_sql, _ = prepare(sql)
        params = list(params) if params else []
        target = self._conn_or_pool()
        return await target.fetchrow(pg_sql, *params)

    async def fetchall(self, sql: str, params: tuple | None = None) -> list[Any]:
        if self._pool is None:
            raise RuntimeError("PostgresDatabase не ініціалізовано")
        pg_sql, _ = prepare(sql)
        params = list(params) if params else []
        target = self._conn_or_pool()
        return await target.fetch(pg_sql, *params)

    async def insert(self, sql: str, params: tuple | None = None) -> int | None:
        result = await self.execute(sql, params)
        return result.lastrowid

    async def update(self, sql: str, params: tuple | None = None) -> int:
        result = await self.execute(sql, params)
        return result.rowcount

    async def delete(self, sql: str, params: tuple | None = None) -> int:
        result = await self.execute(sql, params)
        return result.rowcount

    @asynccontextmanager
    async def transaction(self):
        if self._pool is None:
            raise RuntimeError("PostgresDatabase не ініціалізовано")
        conn = await self._pool.acquire()
        prev = self._tx_conn
        self._tx_conn = conn
        try:
            tr = conn.transaction()
            await tr.start()
            try:
                yield conn
            except BaseException:
                await tr.rollback()
                raise
            await tr.commit()
        finally:
            self._tx_conn = prev
            await self._pool.release(conn)

    async def _version(self) -> int:
        if self._pool is None:
            return 0
        try:
            v = await self._pool.fetchval(
                "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1"
            )
            return int(v) if v is not None else 0
        except Exception:
            return 0

    async def close(self) -> None:
        if self._pool is not None:
            try:
                await self._pool.close()
            except Exception as e:
                logger.warning("error_closing_pg_pool", error=str(e))
            self._pool = None
        self._is_initialized = False
        logger.info("pg_closed")