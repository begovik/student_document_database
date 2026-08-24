"""Failover-обгортка: віддалена PostgreSQL з локальним SQLite як резервом.

Поведінка (згідно з ТЗ):
- при старті перевіряється доступ до віддаленої БД; якщо недоступна —
  робота йде на локальній;
- у remote-режимі кожна DML-операція дублюється у локальну SQLite
  (дзеркало) з тими самими id, тож обидві копії даних завжди актуальні;
- якщо запит до віддаленої БД не проходить, робиться кілька спроб
  (`retries`), після чого — перемикання на локальну (дані вже там, бо
  дзеркало підтримувалось у реальному часі);
- фоновий restore-probe повертає роботу на віддалену БД, щойно вона
  знову доступна, попередньо зливаючи (replay) зміни з локальної через
  outbox-таблицю та відновлюючи локальне дзеркало з remote за потреби.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from typing import Any

import structlog

from harvester.config import DatabaseConfig, get_settings
from harvester.db.connection import Database, SqliteDatabase
from harvester.db.dialect import inject_id, insert_id_table

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

# Таблиці додатка (не службові), що дзеркалюються між local та remote.
APP_TABLES = (
    "domains",
    "sources",
    "documents",
    "document_mirrors",
    "document_refs",
    "extractions",
    "fetch_attempts",
    "tasks",
    "search_queries",
    "topics",
    "document_topics",
    "blacklist",
    "channel_stats",
    "system_events",
    "settings",
)

# Джерело id для рядків, створених у local-режимі: 2e9+ ніколи не конфліктує
# з id, які встигла видати remote-БД (серійні колонки реально < 1e9),
# і replay у remote виконується з тими самими id (FK-цілісність).
LOCAL_ID_BASE = 2_000_000_000

MIRROR_BATCH = 500


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
        self._mirror_task: asyncio.Task | None = None
        self._is_initialized = False
        self._remote_ever_ok = False
        self._local_drift = False
        self._tx_buf: list[tuple[Any, Any, Any, Any]] | None = None
        self.db_path = self.cfg.local_db_path

    @property
    def strict_remote(self) -> bool:
        """Remote-only режим: без дзеркала та без failover на локальну БД."""
        return self.cfg.mode == "remote"

    # ------------------------------------------------------------------ setup
    async def initialize(self, sync_mirror: bool = True) -> None:
        """Ініціалізує БД та, за замовчуванням, синхронізує дзеркало.

        Робочий процес Harvester використовує `sync_mirror=True`. Сервісні
        read-only команди CLI можуть передати `False`, щоб не запускати
        тривалий resync паралельно з уже працюючим процесом.
        """
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

        if self.strict_remote and not remote_ok:
            raise RuntimeError(
                f"mode=remote: віддалена БД недоступна ({self.cfg.host}); "
                "локальний fallback вимкнено"
            )

        if remote_ok:
            self._mode = "remote"
            logger.info("db_mode_remote", host=self.cfg.host or "", strict=self.strict_remote)
            if sync_mirror and not self.strict_remote:
                # Якщо попередній запуск завершився під час аварії — спершу злити outbox.
                try:
                    pending = await self.pending_outbox_count()
                    if pending:
                        logger.info("db_startup_outbox_found", pending=pending)
                    drained = await self._drain_outbox()
                    if drained:
                        logger.info("db_startup_outbox_drained", ops=drained)
                    # Синхронізація при старті: локальне дзеркало повністю
                    # перебудовується з remote (outbox уже порожній), щоб дані
                    # завжди були свіжими.
                    try:
                        await self._ensure_local_mirror(force=True)
                    except Exception as e:
                        logger.error("db_startup_mirror_check_failed", error=str(e))
                    self._start_mirror_loop()
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
                drained = await self._drain_outbox()
                if drained:
                    logger.info("db_restore_drain_progress", ops_drained=drained)
                await self._try_finalize_switch()
            except Exception as e:
                logger.error(
                    "db_restore_merge_error",
                    error=str(e)[:500],
                    outbox_remaining=await self.pending_outbox_count(),
                )

    async def _try_finalize_switch(self) -> None:
        """Фінальний прохід злиття під локом, потім — перемикання на remote.

        Outbox порожній → оновлюється локальне дзеркало (якщо відстало),
        і лише тоді режим змінюється на remote.
        """
        async with self._switch_lock:
            if self._mode != "local":
                return
            await self._drain_outbox()
            row = await self.local.fetchone(f"SELECT COUNT(*) AS c FROM {OUTBOX_TABLE}")
            remaining = row["c"] if row else 0
            if remaining:
                logger.info("db_switch_pending_outbox", remaining=remaining)
                return
            ok = await self._ensure_local_mirror(force=True)
            if not ok:
                logger.warning("db_switch_mirror_failed")
                return
            self._mode = "remote"
            self._start_mirror_loop()
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
                    if self.strict_remote:
                        raise
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
                    if self.strict_remote:
                        raise
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
        """Транзакційний контекст.

        Remote-режим: кадр транзакції remote; дзеркальні записи буферизуються
        і виконуються у local лише після успішного COMMIT (інакше rollback
        remote залишив би зайві рядки у локальному дзеркалі).
        """
        if self._mode != "remote" or self.remote is None:
            active = self._active()
            if active is None:
                yield None
                return
            async with active.transaction() as conn:
                yield conn
            return

        if self._tx_buf is not None:
            # Вкладений контекст: підтвердження — на зовнішньому кадрі.
            async with self._remote_tx_conn() as conn:
                yield conn
            return

        buf: list[tuple[Any, Any, Any, Any]] = []
        self._tx_buf = buf
        try:
            async with self.remote.transaction() as conn:
                yield conn
            for op, sql, params, lid in buf:
                await self._mirror_local(op, sql, params, lid=lid)
        finally:
            self._tx_buf = None

    @asynccontextmanager
    async def _remote_tx_conn(self):
        """Доступ до поточного з'єднання remote-транзакції (для вкладення)."""
        conn = getattr(self.remote, "_tx_conn", None)
        yield conn

    async def _route(self, op: str, sql: str, params: Any = None) -> Any:
        """Єдина точка входу для DML-операцій (під локом перемикання).

        Remote-режим: remote — джерело істини, операція виконується там,
        після чого дублюється у локальне дзеркало (з тими самими id).
        Local-режим: операція виконується локально та записується в outbox
        для подальшого replay у remote.
        """
        async with self._switch_lock:
            if self.local is None:
                raise RuntimeError("FailoverDatabase не ініціалізовано")

            if self._is_local_only_sql(sql):
                active = self._active()
                return await self._exec_on(active, op, sql, params)

            if self._mode == "remote":
                try:
                    result = await self._remote_retry(op, sql, params)
                except RemoteUnavailable:
                    if self.strict_remote:
                        logger.critical(
                            "db_remote_unavailable_strict", host=self.cfg.host or "", op=op
                        )
                        raise
                    await self._downgrade()
                else:
                    lid = None
                    if op == "insert":
                        lid = result if isinstance(result, int) else None
                    else:
                        lid = getattr(result, "lastrowid", None)
                    if not self.strict_remote:
                        if self._tx_buf is not None:
                            self._tx_buf.append((op, sql, params, lid))
                        else:
                            await self._mirror_local(op, sql, params, lid=lid)
                    return result

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

    async def _mirror_local(self, op: str, sql: str, params: Any = None, lid: int | None = None) -> None:
        """Дзеркалює успішно виконану на remote операцію у локальну SQLite.

        Для INSERT у таблицю з серійною колонкою `id`, де remote повернув
        згенерований id, у локальний SQL підставляється той самий `id`
        (дзеркало зберігає тотожні id — FK-ланцюжки залишаються валідними,
        і failover дає ідентичну копію даних).

        Невдача дзеркала НЕ впливає на результат операції (remote успішний):
        позначається `_local_drift`, і дзеркало буде відновлено з remote
        (`_ensure_local_mirror`).
        """
        if self.local is None:
            return

        mirror_sql = sql
        if lid is not None and insert_id_table(sql) is not None:
            injected = inject_id(sql, lid)
            if injected is not None:
                mirror_sql = injected

        try:
            await self._exec_on(self.local, op, mirror_sql, params)
        except Exception as e:
            self._local_drift = True
            logger.error("db_mirror_local_failed", op=op, error=str(e)[:300])

    async def _mirror_counts_mismatch(self) -> str | None:
        """Перша таблиця, де local-дзеркало розходиться з remote за кількістю рядків."""
        if self.remote is None or self.local is None:
            return None
        for table in APP_TABLES:
            r = await self.remote.fetchone(f"SELECT COUNT(*) AS c FROM {table}")
            l = await self.local.fetchone(f"SELECT COUNT(*) AS c FROM {table}")
            rc = r["c"] if r else 0
            lc = l["c"] if l else 0
            if rc != lc:
                return table
        return None

    async def _ensure_local_mirror(self, force: bool = False) -> bool:
        """Перевіряє і (за потреби) відновлює локальне дзеркало з remote.

        Викликається при порожньому outbox (старт/restore, під `_switch_lock`):
        local буде перебудовано з remote повністю.

        `force=True` — завжди перебудовувати (старт додатка: дані мають бути
        свіжими). Інакше — лише коли є розбіжність за кількістю рядків або
        була зафіксована невдача дзеркала (`_local_drift`).

        Захист: якщо remote порожня (наприклад, первинний `db-seed` ще не
        виконано), local НЕ чіпається — він є джерелом для seed.
        """
        if self.remote is None or self.local is None:
            return True
        try:
            if not force:
                mismatch = await self._mirror_counts_mismatch()
                if mismatch is None and not self._local_drift:
                    return True

            remote_any = False
            for table in APP_TABLES:
                r = await self.remote.fetchone(f"SELECT COUNT(*) AS c FROM {table}")
                if r and r["c"]:
                    remote_any = True
                    break
            if not remote_any:
                logger.info(
                    "db_mirror_skip_remote_empty",
                    force=force,
                )
                return True

            await self._resync_local_from_remote()
        except Exception as e:
            self._local_drift = True
            logger.error("db_local_mirror_check_failed", error=str(e)[:300])
            return False
        return True

    async def _resync_local_from_remote(self) -> None:
        """Перебудовує local-дзеркало з remote (delete all + copy, зберігаючи id)."""
        logger.warning(
            "db_local_resync_start",
            path=str(self.local.db_path if self.local else ""),
        )
        await self.local.execute("PRAGMA foreign_keys=OFF")
        try:
            for table in reversed(APP_TABLES):
                await self.local.execute(f"DELETE FROM {table}")
            for table in APP_TABLES:
                await self._copy_remote_table_to_local(table)
        finally:
            await self.local.execute("PRAGMA foreign_keys=ON")
        self._local_drift = False
        logger.info("db_local_resynced")

    async def _copy_remote_table_to_local(self, table: str) -> None:
        """Копіює одну таблицю з remote у local батчами (keyset-пагінація за id)."""
        sample = await self.remote.fetchone(f"SELECT * FROM {table} LIMIT 1")
        if sample is None:
            return
        cols = list(dict(sample).keys())
        col_sql = ", ".join(f'"{c}"' for c in cols)
        placeholders = ", ".join("?" for _ in cols)
        insert_sql = f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})"

        if "id" in cols:
            order, where = "id", "id > ?"
            last_id = 0
            while True:
                rows = await self.remote.fetchall(
                    f"SELECT * FROM {table} WHERE {where} ORDER BY {order} LIMIT ?",
                    (last_id, MIRROR_BATCH),
                )
                if not rows:
                    break
                batch = [tuple(dict(r)[c] for c in cols) for r in rows]
                await self.local.executemany(insert_sql, batch)
                last_id = dict(rows[-1])["id"]
        elif table == "document_topics":
            last_key: tuple[int, int] | None = None
            while True:
                if last_key is None:
                    rows = await self.remote.fetchall(
                        f"SELECT * FROM {table} ORDER BY document_id, topic_id LIMIT ?",
                        (MIRROR_BATCH,),
                    )
                else:
                    rows = await self.remote.fetchall(
                        f"SELECT * FROM {table} "
                        "WHERE (document_id, topic_id) > (?, ?) "
                        "ORDER BY document_id, topic_id LIMIT ?",
                        (*last_key, MIRROR_BATCH),
                    )
                if not rows:
                    break
                batch = [tuple(dict(r)[c] for c in cols) for r in rows]
                await self.local.executemany(insert_sql, batch)
                d = dict(rows[-1])
                last_key = (d["document_id"], d["topic_id"])
        else:  # settings: один рядок
            rows = await self.remote.fetchall(f"SELECT * FROM {table}")
            if rows:
                batch = [tuple(dict(r)[c] for c in cols) for r in rows]
                await self.local.executemany(insert_sql, batch)

    async def _next_reserved_id(self, table: str) -> int:
        """Наступний id у зарезервованому діапазоні (>= LOCAL_ID_BASE).

        Викликається лише з local-режиму (під `_switch_lock` — послідовно),
        тож MAX+1 без гонок. Якщо у local вже є дзеркальні рядки з малими
        (remote-)id, це не впливає: результат завжди >= LOCAL_ID_BASE.
        """
        row = await self.local.fetchone(
            f"SELECT MAX(id) + 1 AS nid FROM {table}"
        )
        nid = int(row["nid"]) if row and row["nid"] is not None else LOCAL_ID_BASE
        return max(nid, LOCAL_ID_BASE)

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
                # Порушення констрейнтів не зникнуть від повторів — кидаємо одразу.
                if self._is_unique_violation(e) or self._is_fk_violation(e):
                    raise
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
            if self.strict_remote:
                logger.critical("db_remote_unavailable_strict", host=self.cfg.host or "")
                return
            self._mode = "local"
            self._cancel_mirror_loop()
            logger.critical("db_failover_started", host=self.cfg.host or "")
            self._start_restore_loop()

    def _cancel_mirror_loop(self) -> None:
        if self._mirror_task is not None and not self._mirror_task.done():
            self._mirror_task.cancel()
        self._mirror_task = None

    def _start_mirror_loop(self) -> None:
        """Періодична звірка дзеркала у remote-режимі (фонове самовідновлення).

        Локальне дзеркало залишається свіжим, навіть якщо додаток працює
        безперервно: розбіжності (втрачені дзеркальні записи, правки на remote
        з іншого екземпляра) виявляються і виправляються автоматично.
        """
        if self.remote is None:
            return
        if self._mirror_task is None or self._mirror_task.done():
            self._mirror_task = asyncio.create_task(self._mirror_loop())

    async def _mirror_loop(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.restore_probe_interval_s)
            if self._mode != "remote" or self.remote is None:
                return
            async with self._switch_lock:
                if self._mode != "remote":
                    return
                try:
                    await self._ensure_local_mirror()
                except Exception as e:
                    logger.error("db_mirror_loop_failed", error=str(e)[:300])

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
        Рядки з порушенням FK-цілісності (сирітські посилання) також
        пропускаються з попередженням — дані можуть бути відновлені
        при наступному повному resync.
        """
        if self.remote is None:
            return 0
        async with self._drain_lock:
            total = 0
            skipped = 0
            batch_num = 0
            while True:
                rows = await self.local.fetchall(
                    f"SELECT rid, op, sql, params FROM {OUTBOX_TABLE} ORDER BY rid LIMIT 1000"
                )
                if not rows:
                    break
                batch_num += 1
                if batch_num == 1:
                    first_rid = rows[0]["rid"]
                    last_rid = rows[-1]["rid"]
                    logger.info(
                        "db_outbox_drain_start",
                        first_rid=first_rid,
                        last_rid=last_rid,
                        batch_size=len(rows),
                    )
                for row in rows:
                    params = json.loads(row["params"])
                    try:
                        await self._exec_on(self.remote, row["op"], row["sql"], params)
                    except Exception as e:
                        if self._is_unique_violation(e):
                            logger.warning(
                                "db_outbox_dup_skipped", rid=row["rid"], error=str(e)[:200]
                            )
                        elif self._is_fk_violation(e):
                            skipped += 1
                            logger.warning(
                                "db_outbox_fk_skipped",
                                rid=row["rid"],
                                op=row["op"],
                                sql_preview=row["sql"][:120],
                                error=str(e)[:300],
                            )
                        else:
                            logger.error(
                                "db_outbox_replay_error",
                                rid=row["rid"],
                                op=row["op"],
                                sql_preview=row["sql"][:120],
                                error=str(e)[:500],
                            )
                            raise
                    await self.local.execute(
                        f"DELETE FROM {OUTBOX_TABLE} WHERE rid = ?", (row["rid"],)
                    )
                    total += 1
            if total or skipped:
                logger.info("db_outbox_replayed", ops=total, skipped_fk=skipped)
            return total

    @staticmethod
    def _is_unique_violation(e: Exception) -> bool:
        name = type(e).__name__
        if name in ("UniqueViolationError", "IntegrityError"):
            return True
        if _AsyncpgUniqueError is not None and isinstance(e, _AsyncpgUniqueError):
            return True
        return False

    @staticmethod
    def _is_fk_violation(e: Exception) -> bool:
        """Визначає, чи є помилка порушенням FK-цілісності."""
        msg = str(e).lower()
        if "foreign key constraint" in msg or "violates foreign key" in msg:
            return True
        name = type(e).__name__
        if name == "ForeignKeyViolationError":
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

    async def mirror_status(self) -> str:
        """Стан локального дзеркала.

        Значення: 'synced' | 'drift' | 'mismatch:<table>' | 'local-only' | 'n/a'.
        """
        if self.remote is None or self.local is None:
            return "n/a"
        if self._mode != "remote":
            return "local-only"
        mismatch = await self._mirror_counts_mismatch()
        if mismatch:
            return f"mismatch:{mismatch}"
        return "drift" if self._local_drift else "synced"

    async def get_version(self) -> int:
        if self._mode == "remote" and self.remote is not None:
            return await self.remote.get_version()
        if self.local is not None:
            return await self.local.get_version()
        return 0

    async def close(self) -> None:
        if self._mirror_task is not None:
            self._mirror_task.cancel()
            try:
                await self._mirror_task
            except (asyncio.CancelledError, Exception):
                pass
            self._mirror_task = None
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
