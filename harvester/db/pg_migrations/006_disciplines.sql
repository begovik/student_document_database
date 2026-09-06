-- 006: єдина таксономія дисциплін + цілодобовий присвоювач
-- Дисципліни живуть у topics з kind='discipline'; широкі теми залишаються kind='topic'.
ALTER TABLE documents ADD COLUMN IF NOT EXISTS discipline_checked_at TEXT;
ALTER TABLE topics ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'topic';
CREATE INDEX IF NOT EXISTS idx_documents_discipline ON documents(discipline_checked_at) WHERE status='verified';