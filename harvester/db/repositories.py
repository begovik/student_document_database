import hashlib
import json
from datetime import datetime, timedelta
from typing import Any

import structlog

from harvester.db.connection import Database

logger = structlog.get_logger()


class DocumentsRepository:
    def __init__(self, db: Database):
        self.db = db

    async def insert_or_ignore(
        self,
        canonical_url: str,
        title_hint: str | None = None,
        source_id: int | None = None,
        doi: str | None = None,
        isbn: str | None = None,
        openalex_id: str | None = None,
        title: str | None = None,
        authors: list[str] | None = None,
        year: int | None = None,
        publisher: str | None = None,
        language: str | None = None,
        lang_confidence: float | None = None,
        doc_type: str = "other",
        udc: str | None = None,
        page_count: int | None = None,
        size_bytes: int | None = None,
        sha256: str | None = None,
        has_text_layer: bool | None = None,
        is_oa: bool = True,
        oa_status: str | None = None,
        status: str = "discovered",
        duplicate_of: int | None = None,
        needs_review: bool = False,
        huge: bool = False,
        verify_attempts: int = 0,
        next_verify_at: str | None = None,
        first_seen_at: str | None = None,
        verified_at: str | None = None,
        extra: dict | None = None,
        landing_url: str | None = None,
    ) -> int | None:
        if first_seen_at is None:
            first_seen_at = datetime.utcnow().isoformat()

        sql = """
        INSERT OR IGNORE INTO documents (
            canonical_url, landing_url, source_id, doi, isbn, openalex_id,
            title, title_hint, authors, year, publisher, language, lang_confidence,
            doc_type, udc, page_count, size_bytes, sha256, has_text_layer,
            is_oa, oa_status, status, duplicate_of, needs_review, huge,
            verify_attempts, next_verify_at, first_seen_at, verified_at, extra
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            canonical_url,
            landing_url,
            source_id,
            doi,
            isbn,
            openalex_id,
            title,
            title_hint,
            json.dumps(authors) if authors else None,
            year,
            publisher,
            language,
            lang_confidence,
            doc_type,
            udc,
            page_count,
            size_bytes,
            sha256,
            1 if has_text_layer else 0 if has_text_layer is False else None,
            1 if is_oa else 0,
            oa_status,
            status,
            duplicate_of,
            1 if needs_review else 0,
            1 if huge else 0,
            verify_attempts,
            next_verify_at,
            first_seen_at,
            verified_at,
            json.dumps(extra) if extra else None,
        )

        cursor = await self.db.execute(sql, params)
        if cursor.rowcount == 0:
            return None
        return cursor.lastrowid

    async def update_verified(
        self,
        doc_id: int,
        sha256: str,
        size_bytes: int,
        page_count: int,
        language: str,
        lang_confidence: float,
        title: str | None = None,
        authors: list[str] | None = None,
        year: int | None = None,
        publisher: str | None = None,
        doc_type: str = "other",
        udc: str | None = None,
        has_text_layer: bool = True,
        is_oa: bool = True,
        oa_status: str | None = None,
        needs_review: bool = False,
        huge: bool = False,
        verified_at: str | None = None,
        text_sample: str | None = None,
    ) -> None:
        if verified_at is None:
            verified_at = datetime.utcnow().isoformat()

        sql = """
        UPDATE documents SET
            sha256 = ?, size_bytes = ?, page_count = ?, language = ?, lang_confidence = ?,
            title = COALESCE(?, title), authors = COALESCE(?, authors),
            year = COALESCE(?, year), publisher = COALESCE(?, publisher),
            doc_type = ?, udc = ?, has_text_layer = ?, is_oa = ?, oa_status = ?,
            needs_review = ?, huge = ?, verified_at = ?, status = 'verified',
            text_sample = COALESCE(?, text_sample)
        WHERE id = ?
        """
        params = (
            sha256,
            size_bytes,
            page_count,
            language,
            lang_confidence,
            title,
            json.dumps(authors) if authors else None,
            year,
            publisher,
            doc_type,
            udc,
            1 if has_text_layer else 0,
            1 if is_oa else 0,
            oa_status,
            1 if needs_review else 0,
            1 if huge else 0,
            verified_at,
            text_sample,
            doc_id,
        )
        await self.db.execute(sql, params)

    async def update_status(self, doc_id: int, status: str) -> None:
        await self.db.execute("UPDATE documents SET status = ? WHERE id = ?", (status, doc_id))

    async def get_by_id(self, doc_id: int) -> dict | None:
        row = await self.db.fetchone("SELECT * FROM documents WHERE id = ?", (doc_id,))
        return dict(row) if row else None

    async def get_by_canonical_url(self, canonical_url: str) -> dict | None:
        row = await self.db.fetchone(
            "SELECT * FROM documents WHERE canonical_url = ?", (canonical_url,)
        )
        return dict(row) if row else None

    async def get_by_sha256(self, sha256: str) -> dict | None:
        row = await self.db.fetchone("SELECT * FROM documents WHERE sha256 = ?", (sha256,))
        return dict(row) if row else None

    async def get_by_doi(self, doi: str) -> dict | None:
        row = await self.db.fetchone("SELECT * FROM documents WHERE doi = ?", (doi,))
        return dict(row) if row else None

    async def count_by_status(self) -> dict[str, int]:
        rows = await self.db.fetchall(
            "SELECT status, COUNT(*) as count FROM documents GROUP BY status"
        )
        return {row["status"]: row["count"] for row in rows}

    async def count_by_language(self) -> dict[str, int]:
        rows = await self.db.fetchall(
            "SELECT COALESCE(language, 'unknown') as lang, COUNT(*) as count "
            "FROM documents WHERE status = 'verified' GROUP BY lang"
        )
        return {row["lang"]: row["count"] for row in rows}


class DocumentRefsRepository:
    def __init__(self, db: Database):
        self.db = db

    async def insert(
        self,
        document_id: int,
        found_via: str,
        channel: str | None = None,
        query_text: str | None = None,
        ref_url: str | None = None,
    ) -> int:
        now = datetime.utcnow().isoformat()
        sql = """
        INSERT INTO document_refs (document_id, found_via, channel, query_text, ref_url, found_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        return await self.db.insert(sql, (document_id, found_via, channel, query_text, ref_url, now))


class FetchAttemptsRepository:
    def __init__(self, db: Database):
        self.db = db

    async def insert(
        self,
        document_id: int,
        kind: str,
        url: str,
        result_code: str,
        started_at: str | None = None,
        duration_ms: int | None = None,
        http_status: int | None = None,
        bytes: int | None = None,
        error: str | None = None,
    ) -> int:
        if started_at is None:
            started_at = datetime.utcnow().isoformat()

        sql = """
        INSERT INTO fetch_attempts (document_id, kind, url, started_at, duration_ms, http_status, bytes, result_code, error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        return await self.db.insert(
            sql, (document_id, kind, url, started_at, duration_ms, http_status, bytes, result_code, error)
        )


class TasksRepository:
    def __init__(self, db: Database):
        self.db = db

    async def insert(
        self,
        task_type: str,
        payload: dict,
        priority: int = 10,
        run_after: str | None = None,
        max_attempts: int = 5,
    ) -> int | None:
        if run_after is None:
            run_after = datetime.utcnow().isoformat()

        payload_json = json.dumps(payload, sort_keys=True)
        payload_hash = hashlib.sha1(payload_json.encode()).hexdigest()
        now = datetime.utcnow().isoformat()

        sql = """
        INSERT INTO tasks (type, payload, payload_hash, status, priority, attempts, max_attempts, run_after, created_at, updated_at)
        VALUES (?, ?, ?, 'pending', ?, 0, ?, ?, ?, ?)
        ON CONFLICT (type, payload_hash) DO UPDATE SET
            status = 'pending',
            attempts = 0,
            run_after = excluded.run_after,
            updated_at = excluded.updated_at
        WHERE tasks.status = 'done'
        """
        cursor = await self.db.execute(sql, (task_type, payload_json, payload_hash, priority, max_attempts, run_after, now, now))
        return cursor.lastrowid if cursor.lastrowid else None

    async def pick_next(
        self, lease_duration_s: int = 300, task_types: list[str] | None = None
    ) -> dict | None:
        now = datetime.utcnow().isoformat()
        lease_expires = (datetime.utcnow() + timedelta(seconds=lease_duration_s)).isoformat()

        type_filter = ""
        params: list = [now]
        if task_types:
            placeholders = ",".join("?" * len(task_types))
            type_filter = f"AND type IN ({placeholders})"
            params.extend(task_types)

        row = await self.db.fetchone(
            f"""
            SELECT * FROM tasks
            WHERE status = 'pending' AND run_after <= ? {type_filter}
            ORDER BY priority DESC, id ASC
            LIMIT 1
            """,
            tuple(params),
        )
        if not row:
            return None

        task = dict(row)
        cursor = await self.db.execute(
            """
            UPDATE tasks SET status = 'running', attempts = attempts + 1,
                   lease_expires_at = ?, updated_at = ?
            WHERE id = ? AND status = 'pending'
            """,
            (lease_expires, now, task["id"]),
        )
        if cursor.rowcount == 0:
            return None
        return task

    async def count_pending_by_type(self, task_type: str) -> int:
        row = await self.db.fetchone(
            "SELECT COUNT(*) as c FROM tasks WHERE status = 'pending' AND type = ?",
            (task_type,),
        )
        return row["c"] if row else 0

    async def count_by_type(self) -> dict[str, dict[str, int]]:
        rows = await self.db.fetchall(
            "SELECT type, status, COUNT(*) as count FROM tasks GROUP BY type, status"
        )
        result: dict[str, dict[str, int]] = {}
        for row in rows:
            result.setdefault(row["type"], {})[row["status"]] = row["count"]
        return result

    async def complete(self, task_id: int) -> None:
        now = datetime.utcnow().isoformat()
        await self.db.execute(
            "UPDATE tasks SET status = 'done', updated_at = ? WHERE id = ?",
            (now, task_id),
        )

    async def fail(self, task_id: int) -> None:
        now = datetime.utcnow().isoformat()
        await self.db.execute(
            "UPDATE tasks SET status = 'failed', updated_at = ? WHERE id = ?",
            (now, task_id),
        )

    async def return_to_pending(self, task_id: int, delay_s: int = 0) -> None:
        now = datetime.utcnow().isoformat()
        run_after = (datetime.utcnow() + timedelta(seconds=delay_s)).isoformat()
        await self.db.execute(
            "UPDATE tasks SET status = 'pending', run_after = ?, updated_at = ? WHERE id = ?",
            (run_after, now, task_id),
        )

    async def recover_stale_tasks(self) -> int:
        now = datetime.utcnow().isoformat()
        cursor = await self.db.execute(
            """
            UPDATE tasks SET status = 'pending', updated_at = ?
            WHERE status = 'running' AND lease_expires_at < ?
            """,
            (now, now),
        )
        return cursor.rowcount

    async def count_by_status(self) -> dict[str, int]:
        rows = await self.db.fetchall(
            "SELECT status, COUNT(*) as count FROM tasks GROUP BY status"
        )
        return {row["status"]: row["count"] for row in rows}


class DomainsRepository:
    def __init__(self, db: Database):
        self.db = db

    async def insert_or_get(self, host: str) -> int:
        now = datetime.utcnow().isoformat()
        host = host.lower()

        async with self.db.transaction():
            row = await self.db.fetchone("SELECT id FROM domains WHERE host = ?", (host,))
            if row:
                return row["id"]

            sql = """
            INSERT INTO domains (host, first_seen_at, last_seen_at)
            VALUES (?, ?, ?)
            """
            cursor = await self.db.execute(sql, (host, now, now))
            return cursor.lastrowid

    async def get_by_host(self, host: str) -> dict | None:
        row = await self.db.fetchone("SELECT * FROM domains WHERE host = ?", (host.lower(),))
        return dict(row) if row else None

    async def update_last_seen(self, domain_id: int) -> None:
        now = datetime.utcnow().isoformat()
        await self.db.execute(
            "UPDATE domains SET last_seen_at = ? WHERE id = ?",
            (now, domain_id),
        )


class SourcesRepository:
    def __init__(self, db: Database):
        self.db = db

    async def insert(
        self,
        domain_id: int,
        base_url: str,
        source_type: str = "unknown",
        platform: str = "unknown",
        discovered_via: str | None = None,
        topic_hint: str | None = None,
    ) -> int:
        now = datetime.utcnow().isoformat()
        sql = """
        INSERT INTO sources (domain_id, base_url, type, platform, discovered_via, topic_hint, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        return await self.db.insert(
            sql, (domain_id, base_url, source_type, platform, discovered_via, topic_hint, now, now)
        )

    async def get_active(self) -> list[dict]:
        rows = await self.db.fetchall("SELECT * FROM sources WHERE status = 'active'")
        return [dict(row) for row in rows]


class BlacklistRepository:
    def __init__(self, db: Database):
        self.db = db

    async def is_blocked(self, host: str) -> bool:
        host = host.lower()

        row = await self.db.fetchone(
            "SELECT 1 FROM blacklist WHERE kind = 'domain' AND pattern = ?",
            (host,),
        )
        if row:
            return True

        rows = await self.db.fetchall("SELECT pattern FROM blacklist WHERE kind = 'tld'")
        for row in rows:
            if host.endswith(row["pattern"]):
                return True

        return False

    async def add(self, pattern: str, kind: str = "domain", reason: str | None = None) -> int:
        now = datetime.utcnow().isoformat()
        sql = "INSERT INTO blacklist (pattern, kind, reason, created_at) VALUES (?, ?, ?, ?)"
        return await self.db.insert(sql, (pattern, kind, reason, now))


class SettingsRepository:
    def __init__(self, db: Database):
        self.db = db

    async def get(self, key: str) -> str | None:
        row = await self.db.fetchone("SELECT value FROM settings WHERE key = ?", (key,))
        return row["value"] if row else None

    async def set(self, key: str, value: str) -> None:
        now = datetime.utcnow().isoformat()
        sql = """
        INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
        """
        await self.db.execute(sql, (key, value, now))


class ChannelStatsRepository:
    def __init__(self, db: Database):
        self.db = db

    async def increment(
        self,
        channel: str,
        requests: int = 0,
        ok: int = 0,
        errors: int = 0,
        items_found: int = 0,
        items_new: int = 0,
    ) -> None:
        now = datetime.utcnow().replace(minute=0, second=0, microsecond=0).isoformat()

        sql = """
        INSERT INTO channel_stats (channel, ts, requests, ok, errors, items_found, items_new)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(channel, ts) DO UPDATE SET
            requests = requests + excluded.requests,
            ok = ok + excluded.ok,
            errors = errors + excluded.errors,
            items_found = items_found + excluded.items_found,
            items_new = items_new + excluded.items_new
        """
        await self.db.execute(sql, (channel, now, requests, ok, errors, items_found, items_new))

    async def get_summary(self, hours: int = 24) -> list[dict]:
        since = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
        rows = await self.db.fetchall(
            """
            SELECT channel, SUM(requests) as requests, SUM(ok) as ok, SUM(errors) as errors,
                   SUM(items_found) as items_found, SUM(items_new) as items_new
            FROM channel_stats
            WHERE ts >= ?
            GROUP BY channel
            ORDER BY items_new DESC
            """,
            (since,),
        )
        return [dict(row) for row in rows]


class SystemEventsRepository:
    def __init__(self, db: Database):
        self.db = db

    async def log(
        self,
        level: str,
        component: str,
        message: str,
        context: dict | None = None,
    ) -> int:
        now = datetime.utcnow().isoformat()
        sql = """
        INSERT INTO system_events (ts, level, component, message, context)
        VALUES (?, ?, ?, ?, ?)
        """
        return await self.db.insert(
            sql, (now, level, component, message, json.dumps(context) if context else None)
        )

    async def get_recent(self, limit: int = 50) -> list[dict]:
        rows = await self.db.fetchall(
            "SELECT * FROM system_events ORDER BY ts DESC LIMIT ?",
            (limit,),
        )
        return [dict(row) for row in rows]


class SearchQueriesRepository:
    def __init__(self, db: Database):
        self.db = db

    async def insert_if_new(
        self, text: str, engine: str = "ddgs", region: str = "ua-uk", topic_hint: str | None = None
    ) -> int | None:
        sql = """
        INSERT OR IGNORE INTO search_queries (text, engine, region, topic_hint)
        VALUES (?, ?, ?, ?)
        """
        cursor = await self.db.execute(sql, (text, engine, region, topic_hint))
        if cursor.rowcount == 0:
            return None
        return cursor.lastrowid

    async def count(self) -> int:
        row = await self.db.fetchone("SELECT COUNT(*) as c FROM search_queries")
        return row["c"] if row else 0

    async def pick_lru(self) -> dict | None:
        """Найдавніше використаний активний запит без cooldown (LRU)."""
        now = datetime.utcnow().isoformat()
        row = await self.db.fetchone(
            """
            SELECT * FROM search_queries
            WHERE status = 'active' AND (cooldown_until IS NULL OR cooldown_until <= ?)
            ORDER BY last_run_at IS NOT NULL ASC, last_run_at ASC, id ASC
            LIMIT 1
            """,
            (now,),
        )
        return dict(row) if row else None

    async def record_run(self, query_id: int, new_results: int) -> None:
        now = datetime.utcnow().isoformat()
        if new_results > 0:
            await self.db.execute(
                """
                UPDATE search_queries
                SET last_run_at = ?, runs = runs + 1, zero_streak = 0,
                    results_yield = results_yield + ?, cooldown_until = NULL
                WHERE id = ?
                """,
                (now, new_results, query_id),
            )
        else:
            row = await self.db.fetchone(
                "SELECT zero_streak, runs FROM search_queries WHERE id = ?", (query_id,)
            )
            if not row:
                return
            zero_streak = row["zero_streak"] + 1
            cooldown = (datetime.utcnow() + timedelta(hours=24 * zero_streak)).isoformat()
            status = "retired" if (zero_streak >= 3 and row["runs"] >= 5) else "active"
            await self.db.execute(
                """
                UPDATE search_queries
                SET last_run_at = ?, runs = runs + 1, zero_streak = ?,
                    cooldown_until = ?, status = ?
                WHERE id = ?
                """,
                (now, zero_streak, cooldown, status, query_id),
            )

    async def get_top(self, limit: int = 20) -> list[dict]:
        rows = await self.db.fetchall(
            """
            SELECT * FROM search_queries
            ORDER BY results_yield DESC, runs DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(row) for row in rows]


class ExtractionsRepository:
    def __init__(self, db: Database):
        self.db = db

    async def upsert(
        self,
        document_id: int,
        quotations: list[dict],
        summary: dict | None = None,
    ) -> int | None:
        now = datetime.utcnow().isoformat()
        quotations_json = json.dumps(quotations, ensure_ascii=False)
        summary_json = json.dumps(summary, ensure_ascii=False) if summary else None

        sql = """
        INSERT INTO extractions (document_id, quotations, summary, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (document_id) DO UPDATE SET
            quotations = excluded.quotations,
            summary = excluded.summary,
            updated_at = excluded.updated_at
        """
        cursor = await self.db.execute(sql, (document_id, quotations_json, summary_json, now, now))
        return cursor.lastrowid if cursor.lastrowid else None

    async def get_by_document_id(self, document_id: int) -> dict | None:
        row = await self.db.fetchone(
            "SELECT * FROM extractions WHERE document_id = ?",
            (document_id,),
        )
        return dict(row) if row else None

    async def exists_for_document(self, document_id: int) -> bool:
        row = await self.db.fetchone(
            "SELECT 1 FROM extractions WHERE document_id = ?",
            (document_id,),
        )
        return row is not None

    async def count(self) -> int:
        row = await self.db.fetchone("SELECT COUNT(*) as count FROM extractions")
        return row["count"] if row else 0

    async def get_documents_without_extractions(
        self,
        topic_id: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        params: list = []
        topic_join = ""
        if topic_id is not None:
            topic_join = "JOIN document_topics dt ON dt.document_id = d.id AND dt.topic_id = ?"
            params.append(topic_id)

        params.extend([limit, offset])
        rows = await self.db.fetchall(
            f"""
            SELECT d.id, d.canonical_url, d.title
            FROM documents d
            {topic_join}
            LEFT JOIN extractions e ON e.document_id = d.id
            WHERE e.id IS NULL
              AND d.status = 'verified'
            ORDER BY d.id
            LIMIT ? OFFSET ?
            """,
            tuple(params),
        )
        return [dict(row) for row in rows]


class TopicsRepository:
    """Робота з темами (таблиця `topics`)."""

    def __init__(self, db: Database):
        self.db = db

    async def list_all(self) -> list[dict]:
        rows = await self.db.fetchall("SELECT * FROM topics ORDER BY id")
        return [dict(row) for row in rows]

    async def get_by_code(self, code: str) -> dict | None:
        row = await self.db.fetchone("SELECT * FROM topics WHERE code = ?", (code,))
        return dict(row) if row else None

    async def get_by_name(self, name_uk_fragment: str) -> list[dict]:
        rows = await self.db.fetchall(
            "SELECT * FROM topics WHERE name_uk LIKE ? ORDER BY id",
            (f"%{name_uk_fragment}%",),
        )
        return [dict(row) for row in rows]

    async def count(self) -> int:
        row = await self.db.fetchone("SELECT COUNT(*) as count FROM topics")
        return row["count"] if row else 0
