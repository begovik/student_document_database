-- 006: єдина таксономія дисциплін + цілодобовий присвоювач
-- Дисципліни живуть у topics з kind='discipline'; широкі теми залишаються kind='topic'.
ALTER TABLE topics ADD COLUMN kind TEXT NOT NULL DEFAULT 'topic';
-- Мітка часу останньої спроби присвоєння дисциплін документу.
ALTER TABLE documents ADD COLUMN discipline_checked_at TEXT;
CREATE INDEX IF NOT EXISTS idx_documents_discipline ON documents(discipline_checked_at) WHERE status='verified';