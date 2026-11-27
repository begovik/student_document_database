-- 007: захист від stale worker після завершення lease.
ALTER TABLE tasks ADD COLUMN lease_token TEXT;
