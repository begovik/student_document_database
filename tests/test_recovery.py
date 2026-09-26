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
