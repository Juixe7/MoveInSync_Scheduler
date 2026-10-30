-- Keep the scheduler's frequent owner-side manual-trigger poll bounded.
-- The MANUAL marker is part of the existing idempotency key, so no job column
-- or table change is needed.
CREATE INDEX idx_exec_pending_manual_partition_created
    ON job_executions (partition_id, created_at)
    WHERE status = 'PENDING'
      AND idempotency_key LIKE '%:MANUAL:%';
