"""Тести failover-шлюзу: local-only режим, резервні id, outbox."""

import asyncio

import pytest

from harvester.config import DatabaseConfig
from harvester.db.failover import LOCAL_ID_BASE, FailoverDatabase


@pytest.fixture
async def db(tmp_path):
    cfg = DatabaseConfig(mode="local", host="", local_db_path=str(tmp_path / "t.db"))
    fdb = FailoverDatabase(cfg, password="")
    await fdb.initialize()
    yield fdb
    await fdb.close()


@pytest.mark.asyncio
async def test_local_only_mode(db):
    assert db.mode == "local"
    assert db.remote is None


@pytest.mark.asyncio
async def test_reserved_ids(db):
    did = await db.insert(
        "INSERT INTO domains (host, first_seen_at, last_seen_at) VALUES (?, ?, ?)",
        ("example.org", "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
    )
    assert did >= LOCAL_ID_BASE

    docid = await db.insert(
        "INSERT OR IGNORE INTO documents (canonical_url, title, status, first_seen_at) "
        "VALUES (?, ?, ?, ?)",
        ("https://example.org/doc1", "Doc 1", "discovered", "2026-01-01T00:00:00"),
    )
    assert docid >= LOCAL_ID_BASE


@pytest.mark.asyncio
async def test_outbox_records_injected_sql(db):
    await db.insert(
        "INSERT INTO domains (host, first_seen_at, last_seen_at) VALUES (?, ?, ?)",
        ("example.org", "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
    )
    rows = await db.fetchall("SELECT sql FROM failover_outbox")
    assert len(rows) == 1
    assert "(id, host" in rows[0]["sql"]
    assert "2000000000" in rows[0]["sql"]


@pytest.mark.asyncio
async def test_upsert_not_in_outbox_as_insert(db):
    await db.execute(
        "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
        ("heartbeat", "{}", "2026-01-01T00:00:00"),
    )
    rows = await db.fetchall("SELECT sql FROM failover_outbox WHERE sql LIKE '%settings%'")
    assert rows
    assert "(id," not in rows[0]["sql"]


@pytest.mark.asyncio
async def test_executemany_outbox(db):
    docid = await db.insert(
        "INSERT OR IGNORE INTO documents (canonical_url, title, status, first_seen_at) "
        "VALUES (?, ?, ?, ?)",
        ("https://example.org/doc1", "Doc 1", "discovered", "2026-01-01T00:00:00"),
    )
    await db.executemany(
        "INSERT INTO document_refs (document_id, found_via, ref_url, found_at) "
        "VALUES (?, ?, ?, ?)",
        [(docid, "search", "https://example.org/a.pdf", "2026-01-01")],
    )
    rows = await db.fetchall(
        "SELECT op FROM failover_outbox WHERE op = 'executemany'"
    )
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_drain_no_remote_keeps_outbox(db):
    await db.insert(
        "INSERT INTO domains (host, first_seen_at, last_seen_at) VALUES (?, ?, ?)",
        ("example.org", "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
    )
    drained = await db._drain_outbox()
    assert drained == 0
    assert await db.pending_outbox_count() == 1


@pytest.mark.asyncio
async def test_restore_task_not_started_without_remote(db):
    # у local-only (remote не налаштований) фоновий restore-цикл не має працювати
    await asyncio.sleep(0.05)
    assert db._restore_task is None or db._restore_task.done()


@pytest.mark.asyncio
async def test_transaction_commit(db):
    async with db.transaction():
        await db.execute(
            "INSERT INTO domains (host, first_seen_at, last_seen_at) VALUES (?, ?, ?)",
            ("tx.org", "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
        )
    row = await db.fetchone("SELECT host FROM domains WHERE host = ?", ("tx.org",))
    assert row and row["host"] == "tx.org"
