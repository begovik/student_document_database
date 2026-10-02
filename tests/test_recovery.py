"""Regression-тести на виправлені механіки відновлення процесу.

Перевіряється:
- пошуковий канал не маскує помилки backend-ів як порожній результат;
- search_queries не йдуть у вічний retired, помилка дає cooldown;
- upsert задач повертає коректний id і піднімає failed/done;
- retry/defer не втрачають задачу та не витрачають retry-бюджет;
- stale-lease recovery включає рядки без lease;
- shared rate limiter і UTC-скидання денних лімітів;
- deferred-задачі прокидаються без масового requeue;
- supervisor перезапускає впавший worker;
- curator gate вимагає strict-верифікацію;
- новий SQL лишається сумісним із PostgreSQL (production-DB).
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from harvester.classify.ratelimit import DailyLimitExhausted, ModelRateLimiter
from harvester.config import DatabaseConfig
from harvester.core.scheduler import Scheduler
from harvester.db.connection import SqliteDatabase
from harvester.db.failover import FailoverDatabase
from harvester.db.migrations import apply_migrations
from harvester.db.repositories import SearchQueriesRepository, TasksRepository
from harvester.discovery.ddgs_search import DDGSSearchChannel, DDGSSearchError


@pytest.fixture
async def db(tmp_path):
    cfg = DatabaseConfig(mode="local", host="", local_db_path=str(tmp_path / "t.db"))
    fdb = FailoverDatabase(cfg, password="")
    await fdb.initialize()
    await apply_migrations(fdb)
    yield fdb
    await fdb.close()


# ---------------------------------------------------------------- DDGS channel


class _FakeDDGS:
    def __init__(self, behaviour: dict):
        self.behaviour = behaviour
        self.calls: list[str] = []

    def text(self, query: str, **kwargs) -> list[dict]:
        backend = kwargs.get("backend", "duckduckgo")
        self.calls.append(backend)
        action = self.behaviour.get(backend, "empty")
        if action == "ratelimit":
            from ddgs.exceptions import RatelimitException

            raise RatelimitException("429")
        if action == "timeout":
            from ddgs.exceptions import TimeoutException

            raise TimeoutException("timeout")
        if action == "error":
            from ddgs.exceptions import DDGSException

            raise DDGSException("backend down")
        if action == "ok":
            return [{"title": "Result", "href": "https://example.org/a.pdf", "body": "text"}]
        return []


@pytest.mark.asyncio
async def test_ddgs_raises_when_all_backends_fail(monkeypatch):
    channel = DDGSSearchChannel()
    channel.enabled = True
    channel.backends = ["duckduckgo", "bing", "brave"]
    fake = _FakeDDGS({b: "error" for b in channel.backends})
    monkeypatch.setattr("harvester.discovery.ddgs_search.DDGS", lambda *a, **k: fake)

    with pytest.raises(DDGSSearchError):
        async for _ in channel.discover({"query_text": "тест", "region": "ua-uk"}):
            pass


@pytest.mark.asyncio
async def test_ddgs_raises_on_rate_limit_without_results(monkeypatch):
    channel = DDGSSearchChannel()
    channel.enabled = True
    channel.backends = ["duckduckgo", "bing"]
    fake = _FakeDDGS({"duckduckgo": "ratelimit", "bing": "ratelimit"})
    monkeypatch.setattr("harvester.discovery.ddgs_search.DDGS", lambda *a, **k: fake)

    with pytest.raises(DDGSSearchError):
        async for _ in channel.discover({"query_text": "тест", "region": "ua-uk"}):
            pass


@pytest.mark.asyncio
async def test_ddgs_empty_but_successful_backends_is_not_an_error(monkeypatch):
    channel = DDGSSearchChannel()
    channel.enabled = True
    channel.backends = ["duckduckgo", "bing"]
    fake = _FakeDDGS({})
    monkeypatch.setattr("harvester.discovery.ddgs_search.DDGS", lambda *a, **k: fake)

    found = [c async for c in channel.discover({"query_text": "тест", "region": "ua-uk"})]
    assert found == []


@pytest.mark.asyncio
async def test_ddgs_recovers_with_next_backend(monkeypatch):
    channel = DDGSSearchChannel()
    channel.enabled = True
    channel.backends = ["duckduckgo", "bing"]
    fake = _FakeDDGS({"duckduckgo": "ratelimit", "bing": "ok"})
    monkeypatch.setattr("harvester.discovery.ddgs_search.DDGS", lambda *a, **k: fake)

    found = [c async for c in channel.discover({"query_text": "тест", "region": "ua-uk"})]
    assert [c.url for c in found] == ["https://example.org/a.pdf"]


@pytest.mark.asyncio
async def test_ddgs_missing_query_text_raises():
    channel = DDGSSearchChannel()
    channel.enabled = True
    channel.backends = ["duckduckgo"]
    with pytest.raises(ValueError):
        async for _ in channel.discover({"region": "ua-uk"}):
            pass


# ------------------------------------------------------------ search_queries


@pytest.mark.asyncio
async def test_zero_result_query_is_cooled_down_not_retired(db):
    repo = SearchQueriesRepository(db)
    qid = await repo.insert_if_new("перший запит", priority=20)
    assert qid is not None

    for _ in range(5):
        await repo.record_run(qid, 0)

    row = await db.fetchone("SELECT * FROM search_queries WHERE id = ?", (qid,))
    assert row["status"] == "active"
    assert row["cooldown_until"] is not None
    assert await repo.pick_lru() is None


@pytest.mark.asyncio
async def test_cooldown_is_capped_at_24h(db):
    repo = SearchQueriesRepository(db)
    qid = await repo.insert_if_new("запит з cap")
    for _ in range(30):
        await repo.record_run(qid, 0)

    row = await db.fetchone("SELECT * FROM search_queries WHERE id = ?", (qid,))
    cooldown = datetime.fromisoformat(row["cooldown_until"])
    delta_h = (cooldown - datetime.now(UTC).replace(tzinfo=None)).total_seconds() / 3600
    assert 0 < delta_h <= 24.01


@pytest.mark.asyncio
async def test_new_results_clear_cooldown(db):
    repo = SearchQueriesRepository(db)
    qid = await repo.insert_if_new("запит з результатом")
    await repo.record_run(qid, 0)
    await repo.record_run(qid, 3)

    row = await db.fetchone("SELECT * FROM search_queries WHERE id = ?", (qid,))
    assert row["zero_streak"] == 0
    assert row["cooldown_until"] is None
    assert row["status"] == "active"


@pytest.mark.asyncio
async def test_insert_if_new_reactivates_retired_query(db):
    repo = SearchQueriesRepository(db)
    qid = await repo.insert_if_new("старий запит")
    await db.execute("UPDATE search_queries SET status = 'retired' WHERE id = ?", (qid,))

    assert await repo.insert_if_new("старий запит") is None
    row = await db.fetchone("SELECT * FROM search_queries WHERE id = ?", (qid,))
    assert row["status"] == "active"


@pytest.mark.asyncio
async def test_reactivate_retired_is_bounded(db):
    repo = SearchQueriesRepository(db)
    for i in range(10):
        await repo.insert_if_new(f"retired {i}")

    await db.execute("UPDATE search_queries SET status = 'retired'")
    count = await repo.reactivate_retired(limit=3)

    assert count == 3
    active = await db.fetchone(
        "SELECT COUNT(*) as c FROM search_queries WHERE status = 'active'"
    )
    assert active["c"] == 3


@pytest.mark.asyncio
async def test_record_error_sets_cooldown_without_zero_streak(db):
    repo = SearchQueriesRepository(db)
    qid = await repo.insert_if_new("запит з помилкою каналу")
    await repo.record_error(qid, cooldown_s=600)

    row = await db.fetchone("SELECT * FROM search_queries WHERE id = ?", (qid,))
    assert row["status"] == "active"
    assert row["zero_streak"] == 0
    assert row["cooldown_until"] is not None
    assert await repo.pick_lru() is None


# ------------------------------------------------------------------ tasks


@pytest.mark.asyncio
async def test_task_insert_returns_existing_id_after_conflict(db):
    repo = TasksRepository(db)
    payload = {"query_id": 1, "query_text": "x"}
    first = await repo.insert("search", payload)
    assert first is not None

    # pending-конфлікт не має оновити рядок і не повертає новий id
    assert await repo.insert("search", payload) is None

    await db.execute("UPDATE tasks SET status = 'done' WHERE id = ?", (first,))
    second = await repo.insert("search", payload)
    assert second == first

    row = await db.fetchone("SELECT * FROM tasks WHERE id = ?", (first,))
    assert row["status"] == "pending"
    assert row["attempts"] == 0


@pytest.mark.asyncio
async def test_failed_task_can_be_rescheduled(db):
    repo = TasksRepository(db)
    task_id = await repo.insert("probe", {"document_id": 7})
    picked = await repo.pick_next(task_types=["probe"])
    assert picked["id"] == task_id
    assert await repo.fail(task_id) is True

    revived = await repo.insert("probe", {"document_id": 7})
    assert revived == task_id
    row = await db.fetchone("SELECT * FROM tasks WHERE id = ?", (task_id,))
    assert row["status"] == "pending"


@pytest.mark.asyncio
async def test_running_task_is_not_stolen_by_upsert(db):
    repo = TasksRepository(db)
    task_id = await repo.insert("probe", {"document_id": 8})
    picked = await repo.pick_next(task_types=["probe"])
    assert picked["id"] == task_id
    assert picked["attempts"] == 1

    assert await repo.insert("probe", {"document_id": 8}) is None
    row = await db.fetchone("SELECT status, attempts FROM tasks WHERE id = ?", (task_id,))
    assert row["status"] == "running"
    assert row["attempts"] == 1


@pytest.mark.asyncio
async def test_return_to_pending_can_reset_attempts(db):
    repo = TasksRepository(db)
    task_id = await repo.insert("classify", {"document_id": 3}, max_attempts=2)
    await repo.pick_next(task_types=["classify"])

    assert await repo.return_to_pending(task_id, delay_s=0, reset_attempts=True)
    row = await db.fetchone("SELECT * FROM tasks WHERE id = ?", (task_id,))
    assert row["status"] == "pending"
    assert row["attempts"] == 0


@pytest.mark.asyncio
async def test_recover_stale_tasks_includes_rows_without_lease(db):
    repo = TasksRepository(db)
    task_id = await repo.insert("probe", {"document_id": 9})
    await db.execute(
        "UPDATE tasks SET status = 'running', lease_expires_at = NULL, lease_token = NULL "
        "WHERE id = ?",
        (task_id,),
    )

    recovered = await repo.recover_stale_tasks()
    assert recovered == 1
    row = await db.fetchone("SELECT * FROM tasks WHERE id = ?", (task_id,))
    assert row["status"] == "pending"


@pytest.mark.asyncio
async def test_recover_stale_keeps_valid_lease(db):
    repo = TasksRepository(db)
    task_id = await repo.insert("probe", {"document_id": 10})
    await db.execute(
        "UPDATE tasks SET status = 'running', lease_expires_at = ? WHERE id = ?",
        ((datetime.now(UTC).replace(tzinfo=None) + timedelta(minutes=5)).isoformat(), task_id),
    )
    assert await repo.recover_stale_tasks() == 0


# --------------------------------------------------------------- scheduler


@pytest.mark.asyncio
async def test_defer_task_returns_task_without_spending_attempts(db):
    sched = Scheduler(db)
    await sched.start()
    await sched.schedule_task("classify", {"document_id": 42}, max_attempts=1)
    task = await sched.pick_task(task_types=["classify"])
    assert task is not None

    assert await sched.defer_task(task["id"], delay_s=0, lease_token=task["lease_token"])

    row = await db.fetchone("SELECT * FROM tasks WHERE id = ?", (task["id"],))
    assert row["status"] == "pending"
    assert row["attempts"] == 0
    assert await sched.pick_task(task_types=["classify"]) is not None


@pytest.mark.asyncio
async def test_defer_task_ignored_for_stale_lease(db):
    sched = Scheduler(db)
    await sched.start()
    await sched.schedule_task("classify", {"document_id": 43})
    task = await sched.pick_task(task_types=["classify"])

    assert not await sched.defer_task(task["id"], delay_s=0, lease_token="wrong-token")
    row = await db.fetchone("SELECT * FROM tasks WHERE id = ?", (task["id"],))
    assert row["status"] == "running"


@pytest.mark.asyncio
async def test_wake_deferred_search_tasks_is_bounded(db):
    sched = Scheduler(db)
    await sched.start()
    future = (datetime.now(UTC).replace(tzinfo=None) + timedelta(days=2)).isoformat()
    for i in range(5):
        await sched.schedule_task("search", {"query_id": i}, run_after=future)

    woken = await sched.wake_deferred_tasks("search", limit=2)
    assert woken == 2

    row = await db.fetchone(
        "SELECT COUNT(*) as c FROM tasks WHERE type='search' AND run_after > ?",
        (datetime.now(UTC).replace(tzinfo=None).isoformat(),),
    )
    assert row["c"] == 3


# ------------------------------------------------------------- rate limiter


@pytest.mark.asyncio
async def test_shared_rate_limiter_is_shared_between_instances():
    ModelRateLimiter.reset_shared()
    try:
        first = ModelRateLimiter.shared(
            gemini_rpm=15, gemini_rpd=500, gemma_rpm=30, gemma_rpd=14000, gemma_tpm=16000
        )
        second = ModelRateLimiter.shared(
            gemini_rpm=15, gemini_rpd=500, gemma_rpm=30, gemma_rpd=14000, gemma_tpm=16000
        )
        assert first is second
    finally:
        ModelRateLimiter.reset_shared()


@pytest.mark.asyncio
async def test_daily_limit_resets_on_utc_date_change():
    limiter = ModelRateLimiter(gemini_rpm=1000, gemini_rpd=1, gemma_rpm=1000, gemma_rpd=1000)
    await limiter.acquire("m", "gemini")
    with pytest.raises(DailyLimitExhausted):
        await limiter.acquire("m", "gemini")

    limiter._daily_date = (datetime.now(UTC).date() - timedelta(days=1)).isoformat()
    await limiter.acquire("m", "gemini")
    assert limiter.get_stats("m", "gemini")["rpd"] == 1


@pytest.mark.asyncio
async def test_quota_is_scaled_by_number_of_keys():
    limiter = ModelRateLimiter(gemini_rpm=1000, gemini_rpd=2, gemma_rpm=1000, gemma_rpd=1000)
    limiter.register_phase_keys("gemini", 3)
    assert limiter.get_stats("m", "gemini")["rpd_limit"] == 6

    for _ in range(6):
        await limiter.acquire("m", "gemini")
    with pytest.raises(DailyLimitExhausted):
        await limiter.acquire("m", "gemini")


# --------------------------------------------------------- verifier worker


def _verifier_batch_sql() -> str:
    """SQL виборки батчу verifier-а (джерело істини для regression-тестів)."""
    import inspect

    from harvester.verifier.worker import VerifierWorker

    source = inspect.getsource(VerifierWorker.run)
    start = source.index('"""')
    end = source.index('"""', start + 3)
    return source[start + 3 : end]


def test_verifier_batch_respects_next_check_at():
    """Батч має брати лише нові/прострочені джерела, а не весь пул щодня."""
    sql = _verifier_batch_sql()
    assert "vr.next_check_at <= ?" in sql
    assert "vr.checked_at < ?" in sql
    # Старий селектор ігнорував next_check_at і щоразу перевіряв увесь пул.
    assert "d.verifier_checked_at < ?" not in sql
    assert "COALESCE(vr.next_check_at" in sql


def test_verifier_batch_prioritises_records_without_llm_verdict():
    """Записи fail-open (llm_status='error') мають перевірятися заново."""
    sql = _verifier_batch_sql()
    assert "vr.llm_status = 'error'" in sql


# --------------------------------------------------- thinking-model responses


def _parts(*specs):
    """Побудує список parts відповіді Gemini: [(thought, text), ...]."""
    return [{"text": text, "thought": thought} if thought else {"text": text} for thought, text in specs]


async def _response_parts(parts, finish_reason="STOP", thoughts=0):
    """Підміняє httpx-відповідь Gemini та повертає видобутий текст."""

    class _Resp:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {
                "candidates": [
                    {"finishReason": finish_reason, "content": {"parts": parts}}
                ],
                "usageMetadata": {
                    "totalTokenCount": 100,
                    "candidatesTokenCount": 100 - thoughts,
                    "thoughtsTokenCount": thoughts,
                },
            }

    from harvester.classify import llm as llm_mod

    original = llm_mod.httpx.AsyncClient
    llm_mod.httpx.AsyncClient = lambda *a, **kw: _FakeClient(_Resp())
    try:
        client = llm_mod.LLMClient(keys=["k"], models=["m"], service="test")
        client._initialized = True
        return await client._call_gemini_with_wait("prompt", "k", "m")
    finally:
        llm_mod.httpx.AsyncClient = original


class _FakeClient:
    def __init__(self, resp):
        self._resp = resp

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, *args, **kwargs):
        return self._resp


@pytest.mark.asyncio
async def test_no_answer_raises_instead_of_parsing_thought_text():
    """Якщо немає non-thought part — LLMNoAnswer, а не спроба розібрати прозу."""
    from harvester.classify.llm import LLMNoAnswer

    parts = _parts((True, '*   Role: Scientific source verifier.\n*   Goal: ...'))
    with pytest.raises(LLMNoAnswer):
        await _response_parts(parts, finish_reason="MAX_TOKENS", thoughts=2045)


@pytest.mark.asyncio
async def test_answer_is_concatenated_from_all_non_thought_parts():
    """JSON, розбитий на кілька parts, має бути склеєний, а не взятий лише перший."""
    parts = _parts(
        (True, "думки, які не є відповіддю"),
        (False, '{"verdict": "pass", '),
        (False, '"confidence": 0.9}'),
    )
    resp = await _response_parts(parts)
    assert resp.text == '{"verdict": "pass", "confidence": 0.9}'
    assert json.loads(resp.text)["verdict"] == "pass"


def test_generation_config_requests_json_mime_type():
    """responseMimeType='application/json' вмикає structured output."""
    import inspect

    from harvester.classify.llm import LLMClient

    source = inspect.getsource(LLMClient._call_gemini_with_wait)
    assert '"responseMimeType": "application/json"' in source


def test_max_tokens_budget_fits_thinking_models():
    """2048 токенів цілком з'їдав thinking — нижня межа має бути вищою."""
    from harvester.config import LLMConfig

    assert LLMConfig().max_tokens >= 4096
    with pytest.raises(ValidationError):
        LLMConfig(max_tokens=2048)


def test_curator_rejects_pass_without_llm_verdict():
    """strict-pass без LLM-вердикту не є доказом якості."""
    from harvester.curator.preparer import is_document_complete

    doc = _gate_doc()
    doc["verifier_status"] = "pass"
    doc["verifier_result_status"] = "pass"

    doc["llm_status"] = "error"
    passed, reason = is_document_complete(doc, require_strict_pass=True)
    assert passed is False
    assert "LLM" in (reason or "")

    doc["llm_status"] = "pass"
    passed, _ = is_document_complete(doc, require_strict_pass=True)
    assert passed is True


def test_verifier_recheck_schedule_uses_longer_delay_for_fail():
    import inspect

    from harvester.verifier.worker import VerifierWorker

    # Логіка запису результату винесена в _process_document, щоб один
    # збійний документ не обривав увесь батч.
    source = inspect.getsource(VerifierWorker._process_document)
    assert "recheck_days if passed else max(recheck_days, 30)" in source


def test_llm_verifier_passes_min_confidence_to_prompt():
    from harvester.verifier.llm_verifier import MIN_LLM_CONFIDENCE, PROMPT

    assert MIN_LLM_CONFIDENCE > 0
    assert "{min_confidence}" in PROMPT


# ------------------------------------------------------------- catalog data


def test_replacement_query_requires_strict_pass():
    import inspect

    from harvester.curator import verifier as curator_verifier

    sql = inspect.getsource(curator_verifier.find_replacement_candidates)
    assert "d.verifier_status = 'pass'" in sql
    assert "vr.status = 'pass'" in sql
    assert "verifier_result_status" in sql


@pytest.mark.asyncio
async def test_load_extraction_fields_parses_db_json(db):
    from harvester.curator.verifier import _has_catalog_extraction_data, _load_extraction_fields

    now = datetime.now(UTC).replace(tzinfo=None).isoformat()
    await db.execute(
        "INSERT INTO documents (canonical_url, status, first_seen_at) VALUES (?, ?, ?)",
        ("https://example.org/extract.pdf", "verified", now),
    )
    doc_id = int((await db.fetchone("SELECT id FROM documents"))["id"])
    await db.execute(
        "INSERT INTO extractions (document_id, quotations, summary, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (
            doc_id,
            '[{"text": "цитата", "page": 3}]',
            '{"text": "стислий виклад", "key_points": ["теза"]}',
            now,
            now,
        ),
    )

    fields = await _load_extraction_fields(db, doc_id)
    assert fields["quotations"][0]["text"] == "цитата"
    assert fields["summary"]["key_points"] == ["теза"]
    assert _has_catalog_extraction_data(fields) is True


@pytest.mark.asyncio
async def test_load_extraction_fields_empty_for_document_without_extraction(db):
    from harvester.curator.verifier import _has_catalog_extraction_data, _load_extraction_fields

    now = datetime.now(UTC).replace(tzinfo=None).isoformat()
    await db.execute(
        "INSERT INTO documents (canonical_url, status, first_seen_at) VALUES (?, ?, ?)",
        ("https://example.org/no-extract.pdf", "verified", now),
    )
    doc_id = int((await db.fetchone("SELECT id FROM documents"))["id"])

    fields = await _load_extraction_fields(db, doc_id)
    assert fields == {}
    # Заміна без extraction не може пройти catalog validation.
    assert _has_catalog_extraction_data(fields) is False


@pytest.mark.asyncio
async def test_load_extraction_fields_ignores_malformed_json(db):
    from harvester.curator.verifier import _load_extraction_fields

    now = datetime.now(UTC).replace(tzinfo=None).isoformat()
    await db.execute(
        "INSERT INTO documents (canonical_url, status, first_seen_at) VALUES (?, ?, ?)",
        ("https://example.org/bad-extract.pdf", "verified", now),
    )
    doc_id = int((await db.fetchone("SELECT id FROM documents"))["id"])
    await db.execute(
        "INSERT INTO extractions (document_id, quotations, summary, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (doc_id, "{не json", "[1, 2, 3]", now, now),
    )

    fields = await _load_extraction_fields(db, doc_id)
    assert fields == {}


def test_is_document_complete_rejects_legacy_structure_without_text_sample():
    """Legacy-запис без text_sample/text_length не є повнотекстним джерелом."""
    from harvester.curator.preparer import is_document_complete

    doc = {
        "status": "verified",
        "title": "Стара стаття",
        "authors": ["Іваненко Іван Іванович"],
        "language": "uk",
        "canonical_url": "https://example.org/old.pdf",
        "page_count": 12,
        "has_text_layer": 1,
        "extra": {"structure": {"has_references": True, "has_introduction": True}},
    }

    passed, reason = is_document_complete(doc)
    assert passed is False
    assert reason



# -------------------------------------------------------- PostgreSQL parity


def test_recover_stuck_verifying_sql_is_postgres_safe():
    """`text || integer` не існує в PG — конкатенація лише через CAST."""
    import inspect

    from harvester.db.repositories import DocumentsRepository

    sql = inspect.getsource(DocumentsRepository.recover_stuck_verifying)
    assert "|| d.id" not in sql
    assert "|| CAST(d.id AS TEXT) ||" in sql


def test_insert_if_new_avoids_sqlite_scalar_max():
    """У PG MAX(x, y) — агрегат; пріоритет рахуємо в Python."""
    import inspect

    from harvester.db.repositories import SearchQueriesRepository

    sql = inspect.getsource(SearchQueriesRepository.insert_if_new)
    assert "MAX(priority" not in sql
    assert "merged_priority" in sql


def test_verifier_batch_sql_translates_to_postgres_placeholders():
    """Перекладач має дати валідні $N для селектора батчу verifier-а."""
    from harvester.db.dialect import replace_placeholders

    sql = (
        "SELECT d.id FROM documents d "
        "LEFT JOIN verifier_results vr ON vr.document_id = d.id AND vr.profile='strict' "
        "WHERE d.status='verified' AND (vr.checked_at IS NULL "
        "OR (vr.next_check_at IS NOT NULL AND vr.next_check_at <= ?) "
        "OR (vr.next_check_at IS NULL AND vr.checked_at < ?)) LIMIT ?"
    )
    translated = replace_placeholders(sql)
    assert translated.count("$") == 3
    assert "?" not in translated


# ------------------------------------------------------------- supervisor


@pytest.mark.asyncio
async def test_supervisor_restarts_failed_worker():
    from harvester.core.supervisor import Supervisor

    supervisor = Supervisor.__new__(Supervisor)
    supervisor._running = True
    supervisor.event_logger = None
    supervisor._worker_restart_delay_s = 0.01

    starts = 0

    async def failing_worker():
        nonlocal starts
        starts += 1
        if starts >= 3:
            supervisor._running = False
        raise RuntimeError("worker crashed")

    task = supervisor._spawn("test-worker", failing_worker)
    await asyncio.wait_for(task, timeout=5)

    assert starts >= 3


@pytest.mark.asyncio
async def test_supervisor_keeps_running_worker_alive_after_heartbeat_cycle():
    from harvester.core.supervisor import Supervisor

    supervisor = Supervisor.__new__(Supervisor)
    supervisor._running = True
    supervisor.event_logger = None
    supervisor._worker_restart_delay_s = 0.01

    cycles = 0

    async def worker():
        nonlocal cycles
        cycles += 1
        await asyncio.sleep(0.01)
        if cycles >= 3:
            supervisor._running = False

    task = supervisor._spawn("test-worker", worker)
    await asyncio.wait_for(task, timeout=5)

    assert cycles == 3


# ----------------------------------------------------------------- curator


def _gate_doc() -> dict:
    return {
        "status": "verified",
        "title": "Повний науковий документ про методику",
        "authors": ["Іваненко Іван Іванович"],
        "language": "uk",
        "canonical_url": "https://example.org/d.pdf",
        "page_count": 12,
        "has_text_layer": 1,
        "text_sample": "Повний текст джерела. " * 60,
        "extra": {
            "text_length": 24000,
            "structure": {
                "has_references": True,
                "has_introduction": True,
                "has_conclusion": True,
                "structured_sections": True,
                "has_title_page": True,
                "toc_ratio": 0.02,
            },
        },
        # Підтверджений LLM-вердикт — обов'язкова частина strict-pass.
        "llm_status": "pass",
    }


def test_strict_gate_requires_verifier_status():
    from harvester.curator.preparer import is_document_complete

    doc = _gate_doc()
    passed, reason = is_document_complete(doc, require_strict_pass=True)
    assert passed is False
    assert "verifier_status" in (reason or "")

    doc["verifier_status"] = "fail"
    assert is_document_complete(doc, require_strict_pass=True)[0] is False

    doc["verifier_status"] = "pass"
    doc["verifier_result_status"] = "fail"
    passed, reason = is_document_complete(doc, require_strict_pass=True)
    assert passed is False
    assert "verifier_results" in (reason or "")

    doc["verifier_result_status"] = "pass"
    assert is_document_complete(doc, require_strict_pass=True)[0] is True


def test_verifier_gate_not_required_for_plain_completeness_check():
    from harvester.curator.preparer import is_document_complete

    doc = _gate_doc()
    assert is_document_complete(doc)[0] is True


# ----------------------------------------------------------------- sqlite


@pytest.mark.asyncio
async def test_recover_stuck_verifying_is_bounded_and_idempotent(db):
    from harvester.db.repositories import DocumentsRepository

    docs = DocumentsRepository(db)
    now = datetime.now(UTC).replace(tzinfo=None).isoformat()
    for i in range(5):
        await db.execute(
            "INSERT INTO documents (canonical_url, status, first_seen_at) VALUES (?, ?, ?)",
            (f"https://example.org/{i}.pdf", "verifying", now),
        )

    recovered = await docs.recover_stuck_verifying(limit=3)
    assert len(recovered) == 3
    rows = await db.fetchall("SELECT status FROM documents")
    assert sum(1 for r in rows if r["status"] == "verifying") == 2
    assert sum(1 for r in rows if r["status"] == "queued") == 3

    # Повторний виклик не змінює вже відновлені документи
    again = await docs.recover_stuck_verifying(limit=10)
    assert len(again) == 2


@pytest.mark.asyncio
async def test_recover_stuck_verifying_skips_docs_with_active_probe(db):
    from harvester.db.repositories import DocumentsRepository, TasksRepository

    docs = DocumentsRepository(db)
    tasks = TasksRepository(db)
    now = datetime.now(UTC).replace(tzinfo=None).isoformat()
    await db.execute(
        "INSERT INTO documents (canonical_url, status, first_seen_at) VALUES (?, ?, ?)",
        ("https://example.org/live.pdf", "verifying", now),
    )
    doc_id = await db.fetchone("SELECT id FROM documents")
    await tasks.insert("probe", {"document_id": int(doc_id["id"])})

    assert await docs.recover_stuck_verifying(limit=10) == []
    row = await db.fetchone("SELECT status FROM documents")
    assert row["status"] == "verifying"


@pytest.mark.asyncio
async def test_recover_stuck_verifying_ignores_similar_ids(db):
    """`document_id: 12` не має збігатися з `document_id: 312`."""
    from harvester.db.repositories import DocumentsRepository, TasksRepository

    docs = DocumentsRepository(db)
    tasks = TasksRepository(db)
    now = datetime.now(UTC).replace(tzinfo=None).isoformat()
    for doc_id in (12, 312):
        await db.execute(
            "INSERT INTO documents (id, canonical_url, status, first_seen_at) VALUES (?, ?, ?, ?)",
            (doc_id, f"https://example.org/{doc_id}.pdf", "verifying", now),
        )
    await tasks.insert("probe", {"document_id": 312})

    recovered = await docs.recover_stuck_verifying(limit=10)
    assert recovered == [12]


def test_ddgs_empty_result_is_not_treated_as_backend_error():
    """«No results found.» — це валідний нуль, а не збій backend-а.

    ddgs піднімає DDGSException("No results found.") коли всі engine-и
    відпрацювали чисто, але нічого не знайшли. Якщо вважати це помилкою,
    запит отримує 30-хв error-cooldown замість zero_streak-сходинки, і на
    пулі з тисяч запитів реальні збої пошуку губилися в шумі.
    """
    from ddgs.exceptions import DDGSException, RatelimitException, TimeoutException

    from harvester.discovery.ddgs_search import _is_no_results

    assert _is_no_results(DDGSException("No results found.")) is True
    # Збіг має бути точним: помилка engine-а зі словами «no results» —
    # це реальний збій, і його не можна вважати нормою.
    assert _is_no_results(DDGSException("Error in engine brave: no results attribute")) is False
    assert _is_no_results(RatelimitException("rate limit")) is False
    assert _is_no_results(TimeoutException("timed out")) is False


def test_ddgs_search_error_only_when_backend_actually_failed():
    """DDGSSearchError лише коли є справжній збій, не при нульовому результаті."""
    import inspect

    from harvester.discovery.ddgs_search import DDGSSearchChannel

    source = inspect.getsource(DDGSSearchChannel.discover)
    assert "if not results and (errors or rate_limited):" in source
    assert "ddgs_all_backends_empty" in source
    assert "_is_no_results" in source


def test_verifier_focus_first_seen_after_default_is_off():
    """Фокус вимкнено за замовчуванням — інакше він зміниться для всіх.

    Null має перетворитися на '1970-01-01' (умова завжди справжня), а не
    на поточну дату: інакше документи, знайдені до вмикання фокусу,
    раптом перестали б перевірятися.
    """
    from harvester.config import VerifierConfig

    assert VerifierConfig().focus_first_seen_after is None

    import inspect

    from harvester.verifier import worker

    source = inspect.getsource(worker)
    assert '(focus or "1970-01-01").strip()' in source, (
        "focus_first_seen_after=None має давати нейтральну відсічку 1970-01-01"
    )


def test_verifier_focus_filters_by_first_seen_and_logs_empty_pool():
    """Фокус має фільтрувати SQL і явно логувати порожній пул.

    Мовчання при активному фокусі виглядає так само, як «все перевірено»,
    і легко приймається за зависання воркера — тому потрібен
    verifier_focus_pool_empty.
    """
    import inspect

    from harvester.verifier import worker

    source = inspect.getsource(worker)
    assert "AND d.first_seen_at >= ?" in source, "фокус не потрапив у SELECT"
    assert "verifier_focus_pool_empty" in source
    # Параметр має йти саме в порядку плейсхолдерів у запиті
    assert "(now_iso, cutoff, focus_after, batch_size)" in source


def test_openalex_429_raises_budget_error_not_generic():
    """429 має бути окремим типом помилки, а не загальним Exception.

    Заміряно 02.10.2026: без API-ключа OpenAlex відповідає 429 з тілом
    "Insufficient budget ... resets at midnight UTC". Загальним шляхом
    помилки кожна задача витрачала б спробу, тож за ~25 хвилин ретраїв
    увесь пул задач перейшов би у failed — до сбросу бюджету.
    """
    from harvester.discovery.openalex import OpenAlexBudgetExhausted

    err = OpenAlexBudgetExhausted(5230)
    assert err.retry_after_s == 5230
    # Нижня межа: сервер може не прислати retry-after, і тоді нуль або
    # від'ємне значення відклали б задачу на миттєво — вона знову впала б
    # у 429 і зациклилась би з порожнім очікуванням.
    assert OpenAlexBudgetExhausted(0).retry_after_s >= 60
    assert OpenAlexBudgetExhausted(-5).retry_after_s >= 60


def test_openalex_429_does_not_consume_retry_budget():
    """defer_task має викликатися на вичерпання бюджету, fail_task — ні.

    fail_task витрачає спробу і за 5 × 300 с переводить задачу у failed.
    Тому різниця між defer і fail тут — не стиль, а те, чи переживе
    пул кампанії ніч до сбросу бюджету.
    """
    import inspect

    from harvester.core.workers import DiscoveryWorker

    source = inspect.getsource(DiscoveryWorker._process_task)
    assert "except OpenAlexBudgetExhausted" in source
    assert "defer_task" in source
    # Обробка 429 має бути ПЕРЕД загальним except, інакше вона не досягається
    assert source.index("except OpenAlexBudgetExhausted") < source.index(
        "discovery_task_error"
    )
    # Лог має бути дросованим: 26 задач помилкається одночасно
    assert "log_throttled" in source


def test_openalex_budget_error_is_not_swallowed_by_generic_handler():
    """Впевнюємось, що OpenAlexBudgetExhausted не підміняється під час імпорту.

    `except Exception` перехопив би його, якби клас не імпортувався або
    перейменувався — тоді тест на порядок except-ів був би марним.
    """
    from harvester.core.workers import DiscoveryWorker  # noqa: F401
    from harvester.discovery.openalex import OpenAlexBudgetExhausted

    assert issubclass(OpenAlexBudgetExhausted, Exception)
    assert not issubclass(OpenAlexBudgetExhausted, (asyncio.CancelledError,))


def test_verifier_llm_max_chars_is_actually_used():
    """verifier.llm_max_chars не має бути мертвою конфігурацією.

    Було: промпт жорстко обрізав текст до 3000 знаків, а поле
    llm_max_chars=15000 не читалося ніде — тобто його зміна в config.yaml
    не робила нічого, і про це не сигнализував ніхто.
    """
    import inspect

    from harvester.verifier import llm_verifier

    source = inspect.getsource(llm_verifier.verify_with_llm)
    assert "[:3000]" not in source, "лишився жорсткий обріз 3000 символів"
    assert "[:max_chars]" in source, "текст має обрізатися через max_chars"

    # Значення з конфігу має реально доходити до промпта
    from harvester.config import VerifierConfig

    cfg = VerifierConfig()
    assert cfg.llm_max_chars >= 8000, (
        "менше 8000 символів у sampled-тексті для української статті — це "
        "знову анотація, і вердикт знову буде хибним"
    )
    assert llm_verifier._max_chars() == cfg.llm_max_chars


def _render_prompt(structure: dict | None, text_sample: str = "x" * 500) -> str:
    """Відрендерити промпт так, як це робить verify_with_llm."""
    from harvester.verifier.llm_verifier import PROMPT

    return PROMPT.format(
        title="t", authors="a", language="uk", udc="347.1", page_count=10,
        text_sample=text_sample, max_chars=12000,
        structure=json.dumps(structure, ensure_ascii=False, sort_keys=True)[:1500],
        structure_missing="ТАК — структурні ознаки НЕ ЗІБРАНО" if not structure else "ні",
        topics_list="t", doc_types="article", min_confidence=0.5,
    )


def test_verifier_prompt_tells_llm_sample_is_partial():
    """Промпт має прямо забороняти ототожнювати фрагмент із документом.

    Інакше LLM читає «у фрагменті немає висновків» як «у документі немає
    висновків» і відхиляє повні статті. Саме так з'явилися 16 хибних
    відхилень серед правових документів (усі з коментарем «фрагмент є
    лише анотацією»).
    """
    rendered = _render_prompt(structure={})
    assert "не плутай відсутнє у фрагменті з відсутнім у документі" in rendered
    assert "це НЕ підстава для fail" in rendered
    # Старі правило «немає вступу/висновків/списку джерел → fail» зникло:
    # воно й викликало хибні відхилення.
    assert "Немає вступу/висновків/списку джерел → fail" not in rendered
    # Шаблон має підставляти max_chars, а не містити літерал 3000
    assert "{max_chars}" not in rendered and "3000 знаків" not in rendered


def test_verifier_prompt_flags_missing_structure():
    """Порожня structure має позначатися як «не зібрано», а не «структури немає»."""
    rendered = _render_prompt(structure=None)
    assert "структурні ознаки НЕ ЗІБРАНО" in rendered
    assert "порожня структура НЕ довід відсутності розділів" in rendered

    # Коли структура є — маркер має бути відсутній, інакше LLM ігноруватиме
    # справжні дані парсера
    with_struct = _render_prompt(structure={"has_references": True})
    assert "структурні ознаки НЕ ЗІБРАНО" not in with_struct
    assert "has_references" in with_struct


def test_pipeline_stores_enough_text_for_verdict():
    """documents.text_sample має вистачати для вердикта, а не лише для мови.

    Якщо збережений зразок коротший за verifier.llm_max_chars, LLM
    отримує менше тексту, ніж обіцяє конфіг, — тобто налаштування знову
    стає неправдою, тільки тепер непомітно.
    """
    from harvester.config import VerifierConfig
    from harvester.verify import pipeline

    assert pipeline.TEXT_SAMPLE_CHARS >= 8000
    assert pipeline.TEXT_SAMPLE_CHARS >= VerifierConfig().llm_max_chars


def test_openalex_search_is_query_param_not_filter():
    """`search` — окремий параметр OpenAlex, а НЕ частина filter.

    Заміряно 02.10.2026: запит виду `?search=X&filter=search:X` відповідає
    HTTP 400, тобто кожна тематична задача падала б у failed і конвеєр
    не отримав би жодної знахідки. Тому `_build_filter` не має права
    пропускати search, а discover() має виносити його в params окремо.
    """
    from harvester.discovery.openalex import OpenAlexChannel

    ch = OpenAlexChannel()

    filters = {"search": "неповнолітній договір", "language": "uk", "is_oa": True}
    built = ch._build_filter(dict(filters))

    assert "search:" not in built, f"search просочився у filter: {built}"
    assert "language:uk" in built
    assert "open_access.is_oa:true" in built


def test_openalex_title_and_abstract_filter_strips_commas():
    """Кома — роздільник фільтрів OpenAlex, її не можна пропустити у значенні.

    Інакше `title_and_abstract.search:"a,b"` розпадеться на два фільтри й API
    поверне 400 замість передбачуваного результату.
    """
    from harvester.discovery.openalex import OpenAlexChannel

    ch = OpenAlexChannel()
    built = ch._build_filter({"title_and_abstract": '"minor, contract"', "is_oa": True})

    assert "title_and_abstract.search:" in built
    assert built.count(",") == 1, f"кома змінила структуру filter: {built}"
    assert '"minor contract"' in built

    # Порожнє значення не має створювати dangling-фільтр
    assert ch._build_filter({"title_and_abstract": "  ,  "}) == "open_access.is_oa:true"


def test_openalex_search_task_is_not_restarted_when_exhausted():
    """Тематичний прохід скінченний — не перезапускати безкінечно.

    Без цієї гілки задача з filters.search падала б у ту саму добову
    перезапуск-гілку, що й курсорне сканування. Наступного дня payload
    з cursor="*" збігся б з уже існуючим (UNIQUE(type, payload_hash)) —
    і запит знову проганяв би ті самі результати, витрачаючи квоту OpenAlex
    (10 req/s) і місце в черзі без жодної нової знахідки.
    """
    import inspect

    from harvester.core.workers import DiscoveryWorker

    source = inspect.getsource(DiscoveryWorker._schedule_next_openalex_page)
    assert "openalex_search_exhausted" in source
    assert "filters.get(\"search\")" in source
    # Порядок перевірок важливий: гілка «курсор скінчився» має передувати
    # добовому перезапуску, інакше умова search ніколи не спрацює.
    assert source.index("search") < source.index("restart_at")


def test_openalex_bulk_scan_still_restarts_daily():
    """Регресія: режим сканування без search має лишатись добовим циклом.

    Іначе зміна для кампанії зупинила б основне наповнення пулу.
    """
    from harvester.core.workers import DiscoveryWorker

    scheduled: list[dict] = []

    class _FakeScheduler:
        async def schedule_task(self, task_type, payload, priority=10,
                                run_after=None, max_attempts=5):
            scheduled.append({"payload": payload, "run_after": run_after})
            return 1

    class _FakeSettings:
        class channels:
            class openalex:
                enabled = True

    worker = DiscoveryWorker.__new__(DiscoveryWorker)
    worker.settings = _FakeSettings()
    worker.scheduler = _FakeScheduler()

    bulk_filters = {"language": "uk", "is_oa": True}
    asyncio.run(
        DiscoveryWorker._schedule_next_openalex_page(worker, {"filters": bulk_filters}, None)
    )
    assert len(scheduled) == 1
    assert scheduled[0]["run_after"] is not None, "курсорне сканування має перезапускатися"
    assert scheduled[0]["payload"]["cursor"] == "*"

    # Тематичний: без перезапуску і без нової задачі
    scheduled.clear()
    asyncio.run(
        DiscoveryWorker._schedule_next_openalex_page(
            worker, {"filters": {**bulk_filters, "search": "неповнолітній"}}, None
        )
    )
    assert scheduled == [], "пошуковий прохід не має перезапускатися"

    # Тематичний із наступним курсором — має продовжитись, з пріоритетом задачі
    scheduled.clear()
    asyncio.run(
        DiscoveryWorker._schedule_next_openalex_page(
            worker,
            {"filters": {**bulk_filters, "search": "неповнолітній"}, "priority": 100},
            "CUR1",
        )
    )
    assert len(scheduled) == 1
    assert scheduled[0]["payload"]["cursor"] == "CUR1"
    assert scheduled[0]["payload"]["filters"]["search"] == "неповнолітній"


def test_llm_alert_only_when_whole_chain_fails():
    """Помилка однієї моделі не має сповіщати — ланцюг і далі працює.

    Практика: gemma-4-31b-it падав у 25% випадків, щоразу перехоплювався
    gemma-4-26b-a4b-it, і на пошту йшло 14 листів на годину без жодних
    наслідків для якості роботи. Тривога без наслідків знецінює сигнал.

    Справжню аварію («впало все») ловить notify_llm_all_exhausted — раз на
    6 годин. Тож перевіряємо, що в llm.py більше немає сповіщень на
    помилку окремої моделі/ключа, а сам механізм «все впало» лишився.
    """
    import inspect

    from harvester.classify import llm as llm_mod

    source = inspect.getsource(llm_mod)
    assert "notify_llm_failure" not in source, (
        "з'явився виклик сповіщення на помилку однієї моделі/ключа"
    )
    # Механізм реальної аварії має лишитися
    assert "notify_llm_all_exhausted" in source
    assert "llm_all_limits_exhausted" in source
    # Помилка має лишатися в списку для діагностики, разом з назвою моделі
    assert "errors.append(f\"[{phase}/{model}]" in source


def test_healthy_gemma_model_is_first_in_chain():
    """Зламана модель не мусить стояти першою в ланцюзі.

    gemma-4-26b-a4b-it тримав 99.7% успіху, gemma-4-31b-it — 74.9%
    (деградація на боці Google). Оскільки ланцюг бере першу, що
    відповіла, 26b має бути першою, інакше близько чверті всіх викликів
    падатиме вхолосту і провокуватиме ретраї та сповіщення.
    """
    from harvester.config import LLMConfig

    models = LLMConfig().gemma_models
    assert models[0] == "gemma-4-26b-a4b-it", f"першою має бути стійка модель, маємо {models}"
    assert "gemma-4-31b-it" in models, "резервну модель не можна викидати з ланцюга"


def test_notify_calls_match_signatures():
    """Усі виклики notify_* мають відповідати сигнатурам функцій.

    Регресія: classify/llm.py передавав notify_llm_failure(..., error_type=...),
    але параметра такого немає. Виняток ковтався обгорткою try/except, тому
    конвеєр працював, але сповіщення про збої LLM не надходили НІКОЛИ — про
    це дізнатися можна було лише зі стороннього лічильника llm_notify_failed.

    Перевірка статична (AST), тож не потребує ні БД, ні мережі.
    """
    import ast
    import inspect
    import pathlib

    from harvester.core import notify

    funcs = {
        name: fn
        for name, fn in vars(notify).items()
        if callable(fn)
        and name.startswith("notify_")
        and getattr(fn, "__module__", "") == "harvester.core.notify"
    }
    assert funcs, "не знайдено жодної notify_* функції — перевірка втратила сенс"

    problems: list[str] = []
    root = pathlib.Path(notify.__file__).resolve().parents[1]
    for path in root.rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"), str(path))):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
            if name not in funcs:
                continue
            sig = inspect.signature(funcs[name])
            max_pos = sum(
                1
                for p in sig.parameters.values()
                if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
            )
            if len(node.args) > max_pos:
                problems.append(f"{path.name}:{node.lineno} {name} — зайві позиційні аргументи")
            for kw in node.keywords:
                if kw.arg and kw.arg not in sig.parameters:
                    problems.append(f"{path.name}:{node.lineno} {name} — невідомий аргумент {kw.arg!r}")

    assert not problems, "розбіжності сигнатур notify_*:\n" + "\n".join(problems)


@pytest.mark.asyncio
async def test_sqlite_migration_version_and_tables(tmp_path):
    db = SqliteDatabase(str(tmp_path / "plain.db"))
    await db.initialize()
    await apply_migrations(db)
    try:
        version = await db.get_version()
        assert version >= 7
        tables = {
            r["name"]
            for r in await db.fetchall("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert {"documents", "tasks", "search_queries", "verifier_results", "extractions"} <= tables
    finally:
        await db.close()
