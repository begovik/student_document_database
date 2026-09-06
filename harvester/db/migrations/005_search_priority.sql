-- 005: пріоритет пошукових запитів
ALTER TABLE search_queries ADD COLUMN priority INTEGER NOT NULL DEFAULT 10;
CREATE INDEX IF NOT EXISTS idx_search_queries_priority ON search_queries(priority DESC, last_run_at);
