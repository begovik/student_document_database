"""Тести трансляції SQLite -> PostgreSQL діалекту."""

import pytest

from harvester.db.dialect import (
    crowcount_from_status,
    inject_id,
    insert_id_table,
    prepare,
    prepare_many,
    replace_placeholders,
    split_statements,
    translate_sql,
)


class TestPlaceholders:
    def test_simple(self):
        assert replace_placeholders("SELECT * FROM t WHERE a = ? AND b = ?") == (
            "SELECT * FROM t WHERE a = $1 AND b = $2"
        )

    def test_ignores_string_literals(self):
        sql = "INSERT INTO t (v) VALUES ('? not a placeholder ?')"
        assert replace_placeholders(sql) == sql

    def test_ignores_comments(self):
        sql = "SELECT * FROM t -- ? in comment\nWHERE a = ?"
        assert replace_placeholders(sql) == "SELECT * FROM t -- ? in comment\nWHERE a = $1"

    def test_escaped_quotes(self):
        sql = "SELECT 'it''s ?' AS x, ? AS y"
        assert replace_placeholders(sql) == "SELECT 'it''s ?' AS x, $1 AS y"


class TestTranslate:
    def test_insert_or_ignore(self):
        sql = "INSERT OR IGNORE INTO documents (canonical_url) VALUES (?)"
        out = translate_sql(sql)
        assert "ON CONFLICT DO NOTHING" in out
        assert "OR IGNORE" not in out

    def test_upsert_kept(self):
        sql = (
            "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        )
        assert "ON CONFLICT(key) DO UPDATE" in translate_sql(sql)

    def test_returning_for_id_tables(self):
        sql = "INSERT OR IGNORE INTO sources (domain_id, url) VALUES (?, ?)"
        assert translate_sql(sql).endswith(" RETURNING id")

    def test_no_returning_for_junction(self):
        sql = "INSERT INTO document_topics (document_id, topic_id) VALUES (?, ?)"
        assert "RETURNING" not in translate_sql(sql)

    def test_select_untouched(self):
        sql = "SELECT id, title FROM documents WHERE status = ?"
        assert translate_sql(sql) == "SELECT id, title FROM documents WHERE status = $1"


class TestUpsertQualify:
    def test_unqualified_set_columns_qualified(self):
        sql = (
            "INSERT INTO channel_stats (channel, ts, requests) VALUES (?, ?, ?) "
            "ON CONFLICT(channel, ts) DO UPDATE SET "
            "requests = requests + excluded.requests, ok = ok + excluded.ok"
        )
        out = translate_sql(sql)
        assert "requests = channel_stats.requests + excluded.requests" in out
        assert "ok = channel_stats.ok + excluded.ok" in out

    def test_qualified_only_untouched(self):
        sql = (
            "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at"
        )
        out = translate_sql(sql)
        assert "value = excluded.value" in out
        assert "updated_at = excluded.updated_at" in out

    def test_returning_still_appended(self):
        sql = (
            "INSERT INTO channel_stats (channel, ts, requests) VALUES (?, ?, ?) "
            "ON CONFLICT(channel, ts) DO UPDATE SET requests = requests + excluded.requests"
        )
        out = translate_sql(sql)
        assert "RETURNING id" in out


class TestPrepare:
    def test_rows_mode(self):
        pg, mode = prepare("SELECT 1")
        assert mode == "rows"

    def test_status_mode(self):
        pg, mode = prepare("UPDATE documents SET status = ? WHERE id = ?")
        assert mode == "status"

    def test_prepare_many_strips_returning(self):
        sql = "INSERT OR IGNORE INTO tasks (type, payload) VALUES (?, ?)"
        assert "RETURNING" not in prepare_many(sql)


class TestInjectId:
    def test_plain_insert(self):
        sql = "INSERT INTO domains (host, first_seen_at, last_seen_at) VALUES (?, ?, ?)"
        out = inject_id(sql, 2000000042)
        assert out == (
            "INSERT INTO domains (id, host, first_seen_at, last_seen_at) "
            "VALUES (2000000042, ?, ?, ?)"
        )

    def test_or_ignore(self):
        sql = "INSERT OR IGNORE INTO documents (canonical_url) VALUES (?)"
        out = inject_id(sql, 2000000043)
        assert out.startswith("INSERT OR IGNORE INTO documents (id, canonical_url)")
        assert "2000000043" in out

    def test_upsert_not_injected(self):
        sql = (
            "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        )
        assert inject_id(sql, 5) is None

    def test_non_insert_not_injected(self):
        assert inject_id("UPDATE documents SET status = ? WHERE id = ?", 5) is None

    def test_existing_id_not_injected(self):
        sql = "INSERT INTO documents (id, canonical_url) VALUES (?, ?)"
        assert inject_id(sql, 5) is None

    def test_pg_after_inject(self):
        sql = "INSERT OR IGNORE INTO documents (canonical_url) VALUES (?)"
        pg, mode = prepare(inject_id(sql, 2000000044))
        assert "VALUES (2000000044, $1)" in pg
        assert "ON CONFLICT DO NOTHING" in pg
        assert mode == "rows"


class TestInsertIdTable:
    def test_plain(self):
        assert insert_id_table("INSERT INTO documents (a) VALUES (?)") == "documents"

    def test_or_ignore(self):
        assert insert_id_table("INSERT OR IGNORE INTO tasks (a) VALUES (?)") == "tasks"

    def test_non_id_table(self):
        assert insert_id_table("INSERT INTO document_topics (a, b) VALUES (?, ?)") is None

    def test_non_insert(self):
        assert insert_id_table("UPDATE documents SET a = ?") is None


class TestSplit:
    def test_basic(self):
        sql = "SELECT 1; SELECT 2;"
        assert split_statements(sql) == ["SELECT 1", "SELECT 2"]

    def test_semicolon_in_string(self):
        sql = "INSERT INTO t (v) VALUES ('a;b'); UPDATE t SET v = 'c';"
        assert split_statements(sql) == [
            "INSERT INTO t (v) VALUES ('a;b')",
            "UPDATE t SET v = 'c'",
        ]


class TestRowcount:
    def test_insert_status(self):
        assert crowcount_from_status("INSERT 0 3") == 3

    def test_update_status(self):
        assert crowcount_from_status("UPDATE 5") == 5

    def test_empty(self):
        assert crowcount_from_status("") == 0
