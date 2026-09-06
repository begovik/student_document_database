"""Тести єдиної таксономії дисциплін і присвоювача.

- міграція 006 додає topics.kind та documents.discipline_checked_at;
- seed_discipline_topics створює dis_* рядки та reuse покритих тем;
- load_disciplines повертає всі дисципліни каталогу;
- seed_queries пропускає теми, що покриті дисциплінами;
- DisciplineAssigner._extract_json та _save_result пишуть правильно.
"""

import pytest

from harvester.classify.discipline_assigner import DisciplineAssigner, _extract_json
from harvester.classify.taxonomy import load_disciplines, load_topics, seed_topics
from harvester.db.connection import SqliteDatabase
from harvester.db.failover import FailoverDatabase
from harvester.db.migrations import apply_migrations
from harvester.discovery.querygen import seed_discipline_topics, seed_queries


@pytest.fixture
async def db(tmp_path):
    from harvester.config import DatabaseConfig

    cfg = DatabaseConfig(mode="local", host="", local_db_path=str(tmp_path / "t.db"))
    fdb = FailoverDatabase(cfg, password="")
    await fdb.initialize()
    yield fdb
    await fdb.close()


@pytest.fixture
async def plain_sqlite(tmp_path):
    db = SqliteDatabase(str(tmp_path / "p.db"))
    await db.initialize()
    await apply_migrations(db)
    yield db
    await db.close()


@pytest.mark.asyncio
async def test_migration_006_columns(plain_sqlite):
    v = (await plain_sqlite.fetchone("PRAGMA user_version"))[0]
    assert v >= 6
    tcols = [c["name"] for c in await plain_sqlite.fetchall("PRAGMA table_info(topics)")]
    dcols = [c["name"] for c in await plain_sqlite.fetchall("PRAGMA table_info(documents)")]
    assert "kind" in tcols
    assert "discipline_checked_at" in dcols


@pytest.mark.asyncio
async def test_seed_discipline_topics(plain_sqlite):
    inserted = await seed_topics(plain_sqlite)
    assert inserted == 25

    n = await seed_discipline_topics(plain_sqlite)
    assert n == 252  # 277 - 25 покритих широких тем

    # Повторне засівання ідемпотентне
    n2 = await seed_discipline_topics(plain_sqlite)
    assert n2 == 0

    total = (await plain_sqlite.fetchone("SELECT COUNT(*) c FROM topics"))["c"]
    assert total == 25 + 252

    # Покриті теми залишились kind='topic' і без dis_* дублікатів
    econ = await plain_sqlite.fetchone("SELECT * FROM topics WHERE code='econ'")
    assert econ["kind"] == "topic"
    dup = await plain_sqlite.fetchone("SELECT COUNT(*) c FROM topics WHERE code LIKE 'dis_%' AND lower(name_uk)='економіка'")
    assert dup["c"] == 0


@pytest.mark.asyncio
async def test_load_disciplines_and_topics(plain_sqlite):
    await seed_topics(plain_sqlite)
    await seed_discipline_topics(plain_sqlite)

    broad = await load_topics(plain_sqlite)
    assert len(broad) == 25  # за замовчуванням лише kind='topic'

    disciplines = await load_disciplines(plain_sqlite)
    assert len(disciplines) == 277

    by_code = {t["code"]: t for t in disciplines}
    assert by_code["econ"]["name_uk"] == "Економіка"
    assert by_code["math"]["name_uk"] == "Математика"
    assert by_code["ped"]["name_uk"] == "Педагогіка та освіта"  # аліас каталогу


@pytest.mark.asyncio
async def test_seed_queries_skips_covered_topics(plain_sqlite):
    await seed_topics(plain_sqlite)
    await seed_discipline_topics(plain_sqlite)
    n = await seed_queries(plain_sqlite)
    assert n == 0


def test_extract_json_plain_and_markdown():
    assert _extract_json('{"disciplines": ["math"], "confidence": 0.9}') == {
        "disciplines": ["math"],
        "confidence": 0.9,
    }
    raw = '```json\n{"disciplines": ["econ", "manag"], "confidence": 0.7}\n```'
    data = _extract_json(raw)
    assert data["disciplines"] == ["econ", "manag"]
    assert data["confidence"] == 0.7


@pytest.mark.asyncio
async def test_assigner_save_result(db):
    await seed_topics(db)
    await seed_discipline_topics(db)
    disciplines = await load_disciplines(db)
    code_to_id = {t["code"]: t["id"] for t in disciplines}

    doc_id = await db.insert(
        "INSERT OR IGNORE INTO documents (canonical_url, title, status, first_seen_at) "
        "VALUES (?, ?, ?, ?)",
        ("https://example.org/d", "Тест", "verified", "2026-01-01T00:00:00"),
    )

    assigner = DisciplineAssigner()
    await assigner._save_result(db, doc_id, ["math", "dis_stem"], 0.92, code_to_id, "gemini-3.5-flash-lite")

    rows = await db.fetchall(
        "SELECT topic_id, score FROM document_topics WHERE document_id=? ORDER BY topic_id",
        (doc_id,),
    )
    codes = {code_to_id["math"], code_to_id["dis_stem"]}
    assert {r["topic_id"] for r in rows} == codes

    doc = await db.fetchone("SELECT discipline_checked_at FROM documents WHERE id=?", (doc_id,))
    assert doc["discipline_checked_at"] is not None