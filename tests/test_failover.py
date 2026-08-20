"""Тести failover-шлюзу: local-only режим, резервні id, outbox, dual-write дзеркало."""

import asyncio

import pytest

from harvester.config import DatabaseConfig
from harvester.db.connection import SqliteDatabase
from harvester.db.failover import LOCAL_ID_BASE, FailoverDatabase
from harvester.db.migrations import apply_migrations


@pytest.fixture
async def db(tmp_path):
    cfg = DatabaseConfig(mode="local", host="", local_db_path=str(tmp_path / "t.db"))
    fdb = FailoverDatabase(cfg, password="")
    await fdb.initialize()
    yield fdb
    await fdb.close()


@pytest.fixture
async def dual_db(tmp_path):
    """Failover-шлюз у remote-режимі з фейковою remote = другою SQLite."""
    cfg = DatabaseConfig(mode="local", host="", local_db_path=str(tmp_path / "t.db"))
    fdb = FailoverDatabase(cfg, password="")
    await fdb.initialize()

    fake_remote = SqliteDatabase(str(tmp_path / "remote.db"))
    await fake_remote.initialize()
    await apply_migrations(fake_remote)

    fdb.remote = fake_remote
    fdb._remote_ever_ok = True
    fdb._mode = "remote"
    yield fdb
    await fake_remote.close()
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


# ------------------------------------------------------------------ dual-write

async def _ins(dual_db, host="example.org"):
    return await dual_db.insert(
        "INSERT INTO domains (host, first_seen_at, last_seen_at) VALUES (?, ?, ?)",
        (host, "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
    )


@pytest.mark.asyncio
async def test_remote_mode_insert_written_to_both(dual_db):
    did = await _ins(dual_db)
    assert did >= 1

    r = await dual_db.remote.fetchone(
        "SELECT id, host FROM domains WHERE host = ?", ("example.org",)
    )
    l = await dual_db.local.fetchone(
        "SELECT id, host FROM domains WHERE host = ?", ("example.org",)
    )
    assert r and l
    # однакове id в обох копіях (дзеркало зі збереженням id)
    assert r["id"] == l["id"] == did
    assert r["host"] == l["host"] == "example.org"


@pytest.mark.asyncio
async def test_remote_mode_no_outbox(dual_db):
    await _ins(dual_db)
    assert await dual_db.pending_outbox_count() == 0


@pytest.mark.asyncio
async def test_reserved_id_after_mirrored_rows(db):
    # після дзеркала у local можуть бути рядки з малими (remote-)id —
    # наступний local-id все одно Має бути у зарезервованому діапазоні
    await db.insert(
        "INSERT INTO domains (id, host, first_seen_at, last_seen_at) VALUES (?, ?, ?, ?)",
        (42, "mirror.org", "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
    )
    nid = await db._next_reserved_id("domains")
    assert nid >= LOCAL_ID_BASE
    nid2 = await db._next_reserved_id("domains")
    assert nid2 == nid


@pytest.mark.asyncio
async def test_remote_mode_insert_or_ignore_same_id(dual_db):
    sql = (
        "INSERT OR IGNORE INTO documents (canonical_url, title, status, first_seen_at) "
        "VALUES (?, ?, ?, ?)"
    )
    p = ("https://example.org/doc1", "Doc 1", "discovered", "2026-01-01T00:00:00")
    await dual_db.execute(sql, p)

    r = await dual_db.remote.fetchone("SELECT id FROM documents WHERE canonical_url = ?", (p[0],))
    l = await dual_db.local.fetchone("SELECT id FROM documents WHERE canonical_url = ?", (p[0],))
    assert r and l
    assert r["id"] == l["id"]


@pytest.mark.asyncio
async def test_remote_mode_update_mirrored(dual_db):
    did = await _ins(dual_db)
    await dual_db.update(
        "UPDATE domains SET delay_ms = ? WHERE id = ?", (7000, did)
    )
    assert (await dual_db.remote.fetchone("SELECT delay_ms FROM domains WHERE id = ?", (did,)))["delay_ms"] == 7000
    assert (await dual_db.local.fetchone("SELECT delay_ms FROM domains WHERE id = ?", (did,)))["delay_ms"] == 7000


@pytest.mark.asyncio
async def test_remote_mode_delete_mirrored(dual_db):
    did = await _ins(dual_db)
    await dual_db.delete("DELETE FROM domains WHERE id = ?", (did,))
    assert await dual_db.remote.fetchone("SELECT 1 FROM domains WHERE id = ?", (did,)) is None
    assert await dual_db.local.fetchone("SELECT 1 FROM domains WHERE id = ?", (did,)) is None


@pytest.mark.asyncio
async def test_remote_mode_executemany_mirrored(dual_db):
    cursor = await dual_db.execute(
        "INSERT OR IGNORE INTO documents (canonical_url, title, status, first_seen_at) "
        "VALUES (?, ?, ?, ?)",
        ("https://example.org/doc1", "Doc 1", "discovered", "2026-01-01T00:00:00"),
    )
    doc_id = cursor.lastrowid
    await dual_db.executemany(
        "INSERT INTO document_refs (document_id, found_via, ref_url, found_at) "
        "VALUES (?, ?, ?, ?)",
        [(doc_id, "search", "https://example.org/a.pdf", "2026-01-01")],
    )
    rc = await dual_db.remote.fetchone("SELECT COUNT(*) AS c FROM document_refs")
    lc = await dual_db.local.fetchone("SELECT COUNT(*) AS c FROM document_refs")
    assert rc["c"] == lc["c"] == 1


@pytest.mark.asyncio
async def test_remote_mode_refs_follow_document_id(dual_db):
    r = await dual_db.execute(
        "INSERT OR IGNORE INTO documents (canonical_url, title, status, first_seen_at) "
        "VALUES (?, ?, ?, ?)",
        ("https://example.org/doc1", "Doc 1", "discovered", "2026-01-01T00:00:00"),
    )
    doc_id = r.lastrowid
    await dual_db.insert(
        "INSERT INTO document_refs (document_id, found_via, ref_url, found_at) "
        "VALUES (?, ?, ?, ?)",
        (doc_id, "search", "https://example.org/a.pdf", "2026-01-01"),
    )
    # зовнішній ключ валідний у локальному дзеркалі (той самий id документа)
    row = await dual_db.local.fetchone(
        "SELECT r.id FROM document_refs r "
        "LEFT JOIN documents d ON d.id = r.document_id "
        "WHERE r.document_id = ? AND d.id IS NULL",
        (doc_id,),
    )
    assert row is None


@pytest.mark.asyncio
async def test_mirror_failure_does_not_fail_op(dual_db):
    # закриваємо локальний бекенд — дзеркало не може виконатись
    await dual_db.local.close()

    did = await _ins(dual_db, host="fail.org")
    assert did >= 1  # операція вдалася (remote — джерело істини)
    assert dual_db._local_drift is True

    r = await dual_db.remote.fetchone("SELECT id FROM domains WHERE host = ?", ("fail.org",))
    assert r and r["id"] == did


@pytest.mark.asyncio
async def test_mirror_resync_restores_local(dual_db):
    did = await _ins(dual_db)
    await dual_db.execute(
        "INSERT INTO system_events (ts, level, component, message) VALUES (?, ?, ?, ?)",
        ("2026-01-01T00:00:00", "info", "test", "hello"),
    )

    # псуємо дзеркало напряму
    await dual_db.local.execute("DELETE FROM domains")
    await dual_db.local.execute("DELETE FROM system_events")

    await dual_db._ensure_local_mirror()

    l = await dual_db.local.fetchone("SELECT id FROM domains WHERE id = ?", (did,))
    assert l is not None and l["id"] == did
    c = await dual_db.local.fetchone("SELECT COUNT(*) AS c FROM system_events")
    assert c["c"] == 1
    assert dual_db._local_drift is False


@pytest.mark.asyncio
async def test_mirror_resync_injects_ids_for_topics(dual_db):
    tr = await dual_db.insert(
        "INSERT INTO topics (code, name_uk, name_en) VALUES (?, ?, ?)",
        ("TF", "Тест", "Test"),
    )
    await dual_db.local.execute("DELETE FROM topics")

    await dual_db._ensure_local_mirror()

    row = await dual_db.local.fetchone("SELECT id FROM topics WHERE id = ?", (tr,))
    assert row is not None


@pytest.mark.asyncio
async def test_mirror_skip_when_remote_empty(db):
    # local має дані, remote порожня (ще не засидена) — дзеркало не знищує local
    await db.insert(
        "INSERT INTO domains (host, first_seen_at, last_seen_at) VALUES (?, ?, ?)",
        ("keep.org", "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
    )
    fake_remote = SqliteDatabase(db.local.db_path.parent / "remote_empty.db")
    await fake_remote.initialize()
    await apply_migrations(fake_remote)
    try:
        db.remote = fake_remote
        db._mode = "remote"
        await db._ensure_local_mirror()
        await db._ensure_local_mirror(force=True)
        row = await db.local.fetchone("SELECT host FROM domains WHERE host = ?", ("keep.org",))
        assert row is not None
    finally:
        await fake_remote.close()


@pytest.mark.asyncio
async def test_mirror_force_sync_refreshes_content(dual_db):
    # однакова кількість рядків, але вміст розходиться (правка на remote
    # з іншого екземпляра): force=True (старт додатка) оновлює local.
    await dual_db.remote.execute(
        "INSERT INTO system_events (id, ts, level, component, message) "
        "VALUES (1, '2026-01-01T00:00:00', 'info', 'test', 'fresh')",
    )
    await dual_db.local.execute(
        "INSERT INTO system_events (id, ts, level, component, message) "
        "VALUES (1, '2026-01-01T00:00:00', 'info', 'test', 'stale')",
    )

    await dual_db._ensure_local_mirror(force=True)

    row = await dual_db.local.fetchone("SELECT message FROM system_events WHERE id = 1")
    assert row is not None and row["message"] == "fresh"


@pytest.mark.asyncio
async def test_mirror_no_force_keeps_content_on_same_counts(dual_db):
    # без force (фонова звірка) швидкий шлях не перебудовує local,
    # якщо кількість рядків збігається.
    await dual_db.remote.execute(
        "INSERT INTO system_events (id, ts, level, component, message) "
        "VALUES (1, '2026-01-01T00:00:00', 'info', 'test', 'fresh')",
    )
    await dual_db.local.execute(
        "INSERT INTO system_events (id, ts, level, component, message) "
        "VALUES (1, '2026-01-01T00:00:00', 'info', 'test', 'stale')",
    )

    await dual_db._ensure_local_mirror()

    row = await dual_db.local.fetchone("SELECT message FROM system_events WHERE id = 1")
    assert row is not None and row["message"] == "stale"


@pytest.mark.asyncio
async def test_remote_transaction_buffers_mirror(dual_db):
    # дзеркало застосовується лише після COMMIT
    async with dual_db.transaction():
        await dual_db.execute(
            "INSERT INTO domains (host, first_seen_at, last_seen_at) VALUES (?, ?, ?)",
            ("tx.org", "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
        )
        assert await dual_db.local.fetchone(
            "SELECT 1 FROM domains WHERE host = ?", ("tx.org",)
        ) is None

    assert await dual_db.local.fetchone(
        "SELECT 1 FROM domains WHERE host = ?", ("tx.org",)
        ) is not None
    assert (await dual_db.remote.fetchone(
        "SELECT 1 FROM domains WHERE host = ?", ("tx.org",)
        )) is not None


@pytest.mark.asyncio
async def test_remote_transaction_rollback_skips_mirror(dual_db):
    # fake remote (SQLite autocommit) не може реально зробити rollback — тому
    # перевіряємо суть: буфер дзеркала СКИДАЄТЬСЯ, local не отримує рядок.
    with pytest.raises(RuntimeError):
        async with dual_db.transaction():
            await dual_db.execute(
                "INSERT INTO domains (host, first_seen_at, last_seen_at) VALUES (?, ?, ?)",
                ("rb.org", "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
            )
            raise RuntimeError("boom")

    assert await dual_db.local.fetchone(
        "SELECT 1 FROM domains WHERE host = ?", ("rb.org",)
    ) is None
