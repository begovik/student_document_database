# AGENTS.md — Harvester Repository Guide

> **ГОЛОВНА ЦІЛЬ ПРОЄКТУ:** Збирати **якісні джерела — документи з повним текстом**, інформативні наукові/технічні статті, монографії, посібники, придатні для використання у наукових працях. **Не** тези, не зміст, не анотації, не фрагменти — лише цілісні, структуровані, логічно завершені джерела (титул, вступ/мета, розділи, висновки, список джерел). Ця ціль є пріоритетом над кількістю та має відображатись у всіх рішеннях з відбору, фільтрації та ранжування.

## Quick Commands

```bash
# Development setup
pip install -e ".[dev]"

# Tests (pytest-asyncio auto mode — no markers needed)
pytest
pytest tests/test_dialect.py          # single file
pytest -v                              # verbose

# Linting (ruff, line-length=100, target py312)
ruff check harvester/

# CLI (entry point: harvester.cli:app)
harvester init-db                      # create schema
harvester start                        # run service
harvester doctor                       # self-diagnostics
```

No mypy/pyright configured. No CI pipeline.

## Architecture

- **Fully async** (asyncio). All I/O via `httpx.AsyncClient` (HTTP/2), `aiosqlite`, `asyncpg`. Blocking ops (PDF parse, langid, fuzzy match) run in `asyncio.to_thread()`.
- **Database failover**: `FailoverDatabase` dual-writes to remote PostgreSQL + local SQLite. Falls back to SQLite if PG unreachable; outbox replays later.
- **SQL dialect translation**: Repos write SQLite syntax (`?` placeholders). `db/dialect.py` translates to PG (`$N`, `ON CONFLICT`).
- **Task queue**: All work is rows in `tasks` table. Atomic pick via `UPDATE ... WHERE status='pending'`. No external broker.
- **Circuit breaker**: Per-domain with exponential backoff (5m → 15m → 1h → 6h).
- **Rate limiting**: Global semaphore (32), per-host token bucket (2s default), bandwidth limiter (2 MB/s).

## Key Conventions

- **Ukrainian language**: All code comments, README, docs, CLI help text are in Ukrainian.
- **Line length**: 100 chars (ruff config in `pyproject.toml`).
- **Python target**: 3.12+ (ruff `target-version = "py312"`).
- **No PDF storage**: PDFs downloaded temporarily for verification only. Only metadata + URLs + SHA-256 stored.
- **Russian/Soviet filtering**: Hard-coded blocking of `.ru`, `.su`, `.рф` TLDs, Russian language, Soviet-era sources.

## Testing Patterns

- Only 2 test files exist: `test_dialect.py` (unit) and `test_failover.py` (integration).
- Fixtures use `tmp_path` for isolated SQLite databases.
- `dual_db` fixture creates FailoverDatabase backed by two SQLite instances.
- `respx` available for HTTP mocking but not used in existing tests.
- No snapshot tests, no integration test services required.

## Config & Environment

- Primary config: `config.yaml` (copy from `config.example.yaml`).
- Env vars: `HARVESTER_` prefix, loaded from `.env` file.
- PG password: `HARVESTER_PG_PASSWORD` or `PG_PASS` env var (never in config.yaml).
- Config singleton: `get_settings()` / `reload_settings()`.
- Required field: `contact.email` (used in User-Agent).

## Gotchas

- **SQLite autocommit**: `SqliteDatabase` uses `isolation_level=None`. Each SQL statement is its own transaction. `transaction()` context manager is a no-op.
- **ID range reservation**: Local-mode SQLite assigns IDs starting from `2,000,000,000` to avoid conflicts with remote PG serial columns.
- **Task idempotency**: `UNIQUE(type, payload_hash)` on tasks — re-scheduling same task is a no-op.
- **Stale task recovery**: Heartbeat loop (30s) recovers tasks stuck in `running` with expired leases.
- **LLM fallback chain**: Gemini (3 keys × 2 models) → Gemma (2 models × 3 keys) → OpenRouter.

## Project Structure

```
harvester/
├── core/           # supervisor, scheduler, workers, circuit breaker, ratelimit
├── db/             # SQLite + PostgreSQL, failover, migrations, repositories
├── net/            # HTTP client, anti-SSRF, domain blacklist
├── discovery/      # search channels (ddgs, openalex, crossref, etc.)
├── verify/         # PDF verification, language detection, filters
├── dedup/          # URL normalization
├── classify/       # topic classification (UDC + keywords + LLM)
├── extract/        # LLM-based quotation extraction
├── curator/        # catalog preparation and verification
└── cli.py          # typer CLI (start, status, doctor, export, find, etc.)
```

## CLI Subcommands

| Command | Description |
|---------|-------------|
| `harvester start` | Run supervisor + workers |
| `harvester status` | Show service state |
| `harvester stats --period 24h` | Channel statistics |
| `harvester export -o file.csv` | Export documents |
| `harvester find --topic "..."` | Search by topic |
| `harvester doctor` | Self-diagnostics |
| `harvester init-db` | Initialize schema |
| `harvester db-status` | Database connection state |
| `harvester db-resync` | Force resync local mirror |
| `harvester add-queries --topic "..." --count 600` | Add search queries for new topic |
| `harvester extract run` | LLM quotation extraction |
| `harvester curator prepare` | LLM-curated catalog |
