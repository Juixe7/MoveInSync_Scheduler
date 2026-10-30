-- V1__initial_schema.sql

-- =====================================================
-- TENANTS & AUTH
-- =====================================================

CREATE TABLE tenants (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid (),
    name VARCHAR(255) NOT NULL UNIQUE,
    timezone VARCHAR(64) NOT NULL DEFAULT 'UTC',
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    paused_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid (),
    tenant_id UUID NOT NULL REFERENCES tenants (id),
    username VARCHAR(255) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, username)
);

-- =====================================================
-- JOBS
-- =====================================================

CREATE TABLE jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid (),
    tenant_id UUID NOT NULL REFERENCES tenants (id),
    external_ref_id VARCHAR(255),
    job_type VARCHAR(50) NOT NULL,
    priority VARCHAR(10) NOT NULL DEFAULT 'NORMAL',
    schedule_type VARCHAR(15) NOT NULL,
    cron_expression VARCHAR(100),
    timezone VARCHAR(64),
    next_fire_time TIMESTAMPTZ,
    payload JSONB NOT NULL DEFAULT '{}',
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    misfire_policy VARCHAR(15) NOT NULL DEFAULT 'FIRE_NOW',
    max_retries INT NOT NULL DEFAULT 3,
    max_run_time_sec INT NOT NULL DEFAULT 300,
    cancellable BOOLEAN NOT NULL DEFAULT FALSE,
    parent_job_id UUID REFERENCES jobs (id),
    partition_id INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (
        tenant_id,
        job_type,
        external_ref_id
    )
);

-- The critical index: "give me due jobs for my partitions"
CREATE INDEX idx_jobs_fire ON jobs (partition_id, next_fire_time)
WHERE
    status = 'ACTIVE'
    AND next_fire_time IS NOT NULL;

CREATE INDEX idx_jobs_tenant ON jobs (tenant_id, status);

CREATE INDEX idx_jobs_parent ON jobs (parent_job_id)
WHERE
    parent_job_id IS NOT NULL;

-- =====================================================
-- JOB EXECUTIONS
-- =====================================================

CREATE TABLE job_executions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid (),
    job_id UUID NOT NULL REFERENCES jobs (id),
    tenant_id UUID NOT NULL REFERENCES tenants (id),
    partition_id INT NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL UNIQUE,
    scheduled_fire_time TIMESTAMPTZ NOT NULL,
    actual_start_time TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    attempt_number INT NOT NULL DEFAULT 1,
    fencing_token BIGINT NOT NULL,
    node_id VARCHAR(100) NOT NULL,
    result JSONB,
    error_message TEXT,
    fire_lag_ms BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_exec_job ON job_executions (job_id, scheduled_fire_time);

CREATE INDEX idx_exec_status ON job_executions (status, tenant_id)
WHERE
    status IN ('DEAD_LETTERED', 'RUNNING');

-- =====================================================
-- PARTITION LEASES
-- =====================================================

CREATE TABLE partition_leases (
    partition_id INT PRIMARY KEY,
    node_id VARCHAR(100),
    fencing_token BIGINT NOT NULL DEFAULT 0,
    lease_expires TIMESTAMPTZ,
    acquired_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Pre-populate 64 partitions
INSERT INTO
    partition_leases (partition_id)
SELECT generate_series (0, 63);

-- =====================================================
-- OUTBOX EVENTS
-- =====================================================

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid (),
    aggregate_type VARCHAR(50) NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    published BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_outbox_unpublished ON outbox_events (created_at)
WHERE
    published = FALSE;
-- =====================================================
-- SEED DEMO TENANTS
-- =====================================================
INSERT INTO tenants (id, name, timezone) VALUES
    ('11111111-1111-1111-1111-111111111111', 'Infosys', 'Asia/Kolkata'),
    ('22222222-2222-2222-2222-222222222222', 'Wipro', 'UTC')
ON CONFLICT DO NOTHING;
