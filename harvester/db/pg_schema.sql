-- Harvester Database Schema (PostgreSQL)
-- Version: 001_init (mirror of harvester/db/schema.sql)

CREATE TABLE IF NOT EXISTS domains (
    id              SERIAL PRIMARY KEY,
    host            TEXT NOT NULL UNIQUE,
    status          TEXT NOT NULL DEFAULT 'active',
    fail_streak     INTEGER NOT NULL DEFAULT 0,
    circuit_state   TEXT NOT NULL DEFAULT 'closed',
    circuit_until   TEXT,
    delay_ms        INTEGER NOT NULL DEFAULT 2000,
    robots_txt      TEXT,
    robots_at       TEXT,
    trust           DOUBLE PRECISION NOT NULL DEFAULT 0.5,
    first_seen_at   TEXT NOT NULL,
    last_seen_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sources (
    id              SERIAL PRIMARY KEY,
    domain_id       INTEGER NOT NULL REFERENCES domains(id),
    base_url        TEXT NOT NULL UNIQUE,
    type            TEXT NOT NULL DEFAULT 'unknown',
    platform        TEXT NOT NULL DEFAULT 'unknown',
    oai_endpoint    TEXT,
    sitemap_url     TEXT,
    lang            TEXT,
    country         TEXT,
    topic_hint      TEXT,
    trust_score     DOUBLE PRECISION NOT NULL DEFAULT 0.5,
    status          TEXT NOT NULL DEFAULT 'probing',
    discovered_via  TEXT,
    harvest_interval_min INTEGER NOT NULL DEFAULT 1440,
    last_harvest_at TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sources_status ON sources(status, last_harvest_at);

CREATE TABLE IF NOT EXISTS documents (
    id              SERIAL PRIMARY KEY,
    canonical_url   TEXT NOT NULL UNIQUE,
    landing_url     TEXT,
    source_id       INTEGER REFERENCES sources(id),
    doi             TEXT,
    isbn            TEXT,
    openalex_id     TEXT,
    title           TEXT,
    title_hint      TEXT,
    authors         TEXT,
    year            INTEGER,
    publisher       TEXT,
    language        TEXT,
    lang_confidence DOUBLE PRECISION,
    doc_type        TEXT DEFAULT 'other',
    udc             TEXT,
    page_count      INTEGER,
    size_bytes      INTEGER,
    sha256          TEXT,
    has_text_layer  INTEGER,
    is_oa           INTEGER DEFAULT 1,
    oa_status       TEXT,
    status          TEXT NOT NULL DEFAULT 'discovered',
    duplicate_of    INTEGER REFERENCES documents(id),
    needs_review    INTEGER NOT NULL DEFAULT 0,
    huge            INTEGER NOT NULL DEFAULT 0,
    verify_attempts INTEGER NOT NULL DEFAULT 0,
    next_verify_at  TEXT,
    first_seen_at   TEXT NOT NULL,
    verified_at     TEXT,
    extra           TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_documents_sha256 ON documents(sha256) WHERE sha256 IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_documents_doi    ON documents(doi)    WHERE doi    IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_documents_status   ON documents(status);
CREATE INDEX IF NOT EXISTS idx_documents_lang     ON documents(language, status);
CREATE INDEX IF NOT EXISTS idx_documents_year     ON documents(year);
CREATE INDEX IF NOT EXISTS idx_documents_source   ON documents(source_id, status);
CREATE INDEX IF NOT EXISTS idx_documents_reverify ON documents(next_verify_at) WHERE status='verified';

CREATE TABLE IF NOT EXISTS document_mirrors (
    id           SERIAL PRIMARY KEY,
    document_id  INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    url          TEXT NOT NULL UNIQUE,
    ok           INTEGER NOT NULL DEFAULT 1,
    checked_at   TEXT
);

CREATE TABLE IF NOT EXISTS document_refs (
    id           SERIAL PRIMARY KEY,
    document_id  INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    found_via    TEXT NOT NULL,
    channel      TEXT,
    query_text   TEXT,
    ref_url      TEXT,
    found_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_refs_doc ON document_refs(document_id);

CREATE TABLE IF NOT EXISTS fetch_attempts (
    id           SERIAL PRIMARY KEY,
    document_id  INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    kind         TEXT NOT NULL,
    url          TEXT NOT NULL,
    started_at   TEXT NOT NULL,
    duration_ms  INTEGER,
    http_status  INTEGER,
    bytes        INTEGER,
    result_code  TEXT NOT NULL,
    error        TEXT
);
CREATE INDEX IF NOT EXISTS idx_attempts_doc ON fetch_attempts(document_id, started_at);
CREATE INDEX IF NOT EXISTS idx_attempts_time ON fetch_attempts(started_at);

CREATE TABLE IF NOT EXISTS tasks (
    id           SERIAL PRIMARY KEY,
    type         TEXT NOT NULL,
    payload      TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',
    priority     INTEGER NOT NULL DEFAULT 10,
    attempts     INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    run_after    TEXT NOT NULL,
    lease_expires_at TEXT,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    UNIQUE(type, payload_hash)
);
CREATE INDEX IF NOT EXISTS idx_tasks_pick ON tasks(status, run_after, priority DESC, id);

CREATE TABLE IF NOT EXISTS search_queries (
    id            SERIAL PRIMARY KEY,
    text          TEXT NOT NULL,
    engine        TEXT NOT NULL DEFAULT 'ddgs',
    region        TEXT NOT NULL DEFAULT 'ua-uk',
    topic_hint    TEXT,
    status        TEXT NOT NULL DEFAULT 'active',
    runs          INTEGER NOT NULL DEFAULT 0,
    zero_streak   INTEGER NOT NULL DEFAULT 0,
    results_yield INTEGER NOT NULL DEFAULT 0,
    last_run_at   TEXT,
    cooldown_until TEXT,
    UNIQUE(text, engine, region)
);

CREATE TABLE IF NOT EXISTS topics (
    id            SERIAL PRIMARY KEY,
    code          TEXT NOT NULL UNIQUE,
    name_uk       TEXT NOT NULL,
    name_en       TEXT NOT NULL,
    udc_prefixes  TEXT NOT NULL DEFAULT '[]',
    openalex_field_id INTEGER,
    keywords_uk   TEXT NOT NULL DEFAULT '[]',
    keywords_en   TEXT NOT NULL DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS document_topics (
    document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    topic_id    INTEGER NOT NULL REFERENCES topics(id),
    score       DOUBLE PRECISION NOT NULL,
    signals     TEXT,
    PRIMARY KEY (document_id, topic_id)
);

CREATE TABLE IF NOT EXISTS blacklist (
    id         SERIAL PRIMARY KEY,
    pattern    TEXT NOT NULL UNIQUE,
    kind       TEXT NOT NULL DEFAULT 'domain',
    reason     TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS channel_stats (
    id          SERIAL PRIMARY KEY,
    channel     TEXT NOT NULL,
    ts          TEXT NOT NULL,
    requests    INTEGER NOT NULL DEFAULT 0,
    ok          INTEGER NOT NULL DEFAULT 0,
    errors      INTEGER NOT NULL DEFAULT 0,
    items_found INTEGER NOT NULL DEFAULT 0,
    items_new   INTEGER NOT NULL DEFAULT 0,
    UNIQUE(channel, ts)
);

CREATE TABLE IF NOT EXISTS system_events (
    id         SERIAL PRIMARY KEY,
    ts         TEXT NOT NULL,
    level      TEXT NOT NULL,
    component  TEXT NOT NULL,
    message    TEXT NOT NULL,
    context    TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_ts ON system_events(ts);

CREATE TABLE IF NOT EXISTS settings (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- 004: verifier 24/7
ALTER TABLE documents ADD COLUMN IF NOT EXISTS verifier_status TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS verifier_comment TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS verifier_checked_at TEXT;
CREATE TABLE IF NOT EXISTS verifier_results (
    id              SERIAL PRIMARY KEY,
    document_id     INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    profile         TEXT NOT NULL DEFAULT 'strict',
    status          TEXT NOT NULL,
    comment         TEXT,
    rules_failed    TEXT,
    llm_status      TEXT,
    llm_comment     TEXT,
    llm_model       TEXT,
    llm_key_idx     INTEGER,
    checked_at      TEXT NOT NULL,
    next_check_at   TEXT,
    UNIQUE(document_id, profile)
);
CREATE INDEX IF NOT EXISTS idx_verifier_document ON verifier_results(document_id);
CREATE INDEX IF NOT EXISTS idx_verifier_status ON verifier_results(status);
CREATE INDEX IF NOT EXISTS idx_verifier_next_check ON verifier_results(next_check_at) WHERE status='pass';
CREATE INDEX IF NOT EXISTS idx_documents_verifier ON documents(verifier_checked_at) WHERE status='verified';