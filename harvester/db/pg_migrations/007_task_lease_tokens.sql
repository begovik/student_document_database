-- 007: захист від stale worker після завершення lease.
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS lease_token TEXT;
