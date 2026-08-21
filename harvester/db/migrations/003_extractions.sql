-- 003: таблиця extractions для цитат і сумаризацій
CREATE TABLE IF NOT EXISTS extractions (
    id              INTEGER PRIMARY KEY,
    document_id     INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    quotations      TEXT,
    summary         TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    UNIQUE(document_id)
);
CREATE INDEX IF NOT EXISTS idx_extractions_doc ON extractions(document_id);
