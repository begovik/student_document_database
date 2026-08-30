-- 004: цілодобова перевірка джерел за strict-правилами (LLM-верифікація)
ALTER TABLE documents ADD COLUMN verifier_status TEXT;
ALTER TABLE documents ADD COLUMN verifier_comment TEXT;
ALTER TABLE documents ADD COLUMN verifier_checked_at TEXT;

CREATE TABLE IF NOT EXISTS verifier_results (
    id              INTEGER PRIMARY KEY,
    document_id     INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    profile         TEXT NOT NULL DEFAULT 'strict',
    status          TEXT NOT NULL, -- 'pass' / 'fail' / 'error'
    comment         TEXT,
    rules_failed    TEXT, -- JSON array
    llm_status      TEXT, -- 'pass'/'fail'/'skip'/'error'
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
