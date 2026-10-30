-- Widen status column to accommodate CANCELLED_BEFORE_EXECUTION (26 chars)
ALTER TABLE job_executions ALTER COLUMN status TYPE VARCHAR(50);
