CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS job_definition (
    job_key                  TEXT PRIMARY KEY,
    version                  INTEGER NOT NULL,
    display_name             TEXT NOT NULL,
    description              TEXT,
    owner_team               TEXT NOT NULL,
    tags                     TEXT[] NOT NULL DEFAULT '{}',
    execution_mode           TEXT NOT NULL,
    payload_schema_version   TEXT,
    state                    TEXT NOT NULL,
    manual_triggerable       BOOLEAN NOT NULL DEFAULT FALSE,
    internal_only            BOOLEAN NOT NULL DEFAULT FALSE,
    tenant_scope             TEXT,
    policy_json              JSONB NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL,
    updated_at               TIMESTAMPTZ NOT NULL,
    updated_by               TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS job_schedule (
    job_key                  TEXT PRIMARY KEY REFERENCES job_definition(job_key) ON DELETE CASCADE,
    schedule_kind            TEXT NOT NULL,
    cron_expression          TEXT,
    zone_id                  TEXT NOT NULL DEFAULT 'UTC',
    one_time_at              TIMESTAMPTZ,
    fixed_delay_ms           BIGINT,
    fixed_rate_ms            BIGINT,
    initial_delay_ms         BIGINT,
    effective_from           TIMESTAMPTZ,
    effective_to             TIMESTAMPTZ,
    misfire_policy           TEXT NOT NULL,
    enabled                  BOOLEAN NOT NULL DEFAULT TRUE,
    next_materialize_at      TIMESTAMPTZ,
    last_evaluated_at        TIMESTAMPTZ,
    cursor_version           BIGINT NOT NULL DEFAULT 0,
    updated_at               TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS job_execution (
    execution_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_key                  TEXT NOT NULL REFERENCES job_definition(job_key),
    tenant_id                TEXT,
    trigger_type             TEXT NOT NULL,
    scheduled_at             TIMESTAMPTZ NOT NULL,
    claimable_at             TIMESTAMPTZ NOT NULL,
    claimed_at               TIMESTAMPTZ,
    started_at               TIMESTAMPTZ,
    finished_at              TIMESTAMPTZ,
    status                   TEXT NOT NULL,
    worker_id                TEXT,
    attempt                  INTEGER NOT NULL DEFAULT 1 CHECK (attempt >= 1),
    max_attempts             INTEGER NOT NULL DEFAULT 1 CHECK (max_attempts >= 1),
    payload_ref              TEXT,
    payload_hash             BYTEA,
    result_summary           TEXT,
    error_class              TEXT,
    error_summary            TEXT,
    error_stacktrace         TEXT,
    cancellation_requested   BOOLEAN NOT NULL DEFAULT FALSE,
    cancellation_requested_at TIMESTAMPTZ,
    lease_token              TEXT,
    fencing_token            BIGINT NOT NULL DEFAULT 0,
    lease_expires_at         TIMESTAMPTZ,
    last_heartbeat_at        TIMESTAMPTZ,
    retry_after              TIMESTAMPTZ,
    idempotency_key          TEXT,
    business_key             TEXT,
    dedupe_key               TEXT,
    correlation_id           TEXT,
    trace_id                 TEXT,
    causation_id             TEXT,
    parent_execution_id      UUID REFERENCES job_execution(execution_id),
    created_at               TIMESTAMPTZ NOT NULL,
    updated_at               TIMESTAMPTZ NOT NULL,
    version                  BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS job_execution_attempt (
    attempt_id               BIGSERIAL PRIMARY KEY,
    execution_id             UUID NOT NULL REFERENCES job_execution(execution_id) ON DELETE CASCADE,
    attempt_no               INTEGER NOT NULL CHECK (attempt_no >= 1),
    worker_id                TEXT,
    lease_token              TEXT,
    fencing_token            BIGINT,
    claimed_at               TIMESTAMPTZ,
    started_at               TIMESTAMPTZ,
    finished_at              TIMESTAMPTZ,
    status                   TEXT NOT NULL,
    error_class              TEXT,
    error_summary            TEXT,
    error_stacktrace         TEXT,
    duration_ms              BIGINT,
    created_at               TIMESTAMPTZ NOT NULL,
    UNIQUE (execution_id, attempt_no)
);

CREATE TABLE IF NOT EXISTS job_trigger_request (
    request_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_key                  TEXT NOT NULL REFERENCES job_definition(job_key),
    tenant_id                TEXT,
    payload_ref              TEXT,
    idempotency_key          TEXT,
    trigger_at               TIMESTAMPTZ NOT NULL,
    actor                    TEXT NOT NULL,
    reason                   TEXT,
    state                    TEXT NOT NULL DEFAULT 'PENDING',
    execution_id             UUID REFERENCES job_execution(execution_id),
    requested_at             TIMESTAMPTZ NOT NULL,
    processed_at             TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS job_audit_event (
    event_id                 BIGSERIAL PRIMARY KEY,
    event_type               TEXT NOT NULL,
    job_key                  TEXT REFERENCES job_definition(job_key),
    execution_id             UUID REFERENCES job_execution(execution_id),
    tenant_id                TEXT,
    actor                    TEXT,
    details_json             JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at               TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS job_dead_letter (
    dead_id                  BIGSERIAL PRIMARY KEY,
    execution_id             UUID UNIQUE NOT NULL REFERENCES job_execution(execution_id) ON DELETE CASCADE,
    job_key                  TEXT NOT NULL,
    tenant_id                TEXT,
    reason                   TEXT NOT NULL,
    details_json             JSONB NOT NULL DEFAULT '{}'::jsonb,
    failed_at                TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_job_schedule_next_materialize
    ON job_schedule(next_materialize_at)
    WHERE enabled = TRUE;

CREATE INDEX IF NOT EXISTS ix_job_execution_due_claim
    ON job_execution(claimable_at, scheduled_at)
    WHERE status = 'SCHEDULED' AND cancellation_requested = FALSE;

CREATE INDEX IF NOT EXISTS ix_job_execution_active_lease
    ON job_execution(lease_expires_at)
    WHERE status IN ('CLAIMED', 'RUNNING', 'CANCEL_REQUESTED');

CREATE INDEX IF NOT EXISTS ix_job_execution_running_by_job
    ON job_execution(job_key, tenant_id, status);

CREATE INDEX IF NOT EXISTS ix_job_execution_history_by_job
    ON job_execution(job_key, scheduled_at DESC);

CREATE INDEX IF NOT EXISTS ix_job_execution_retry_after
    ON job_execution(retry_after)
    WHERE status = 'FAILED_RETRYABLE';

CREATE INDEX IF NOT EXISTS ix_job_execution_parent
    ON job_execution(parent_execution_id);

CREATE INDEX IF NOT EXISTS ix_job_execution_attempt_lookup
    ON job_execution_attempt(execution_id, attempt_no DESC);

CREATE INDEX IF NOT EXISTS ix_job_trigger_pending
    ON job_trigger_request(state, trigger_at)
    WHERE state = 'PENDING';

CREATE INDEX IF NOT EXISTS ix_job_audit_by_job
    ON job_audit_event(job_key, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_job_audit_by_execution
    ON job_audit_event(execution_id, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS ux_job_execution_schedule_dedupe
    ON job_execution(job_key, COALESCE(tenant_id, ''), scheduled_at)
    WHERE status IN ('SCHEDULED', 'CLAIMED', 'RUNNING', 'CANCEL_REQUESTED');

CREATE UNIQUE INDEX IF NOT EXISTS ux_job_execution_idempotency
    ON job_execution(job_key, COALESCE(tenant_id, ''), idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_trigger_request_idempotency
    ON job_trigger_request(job_key, COALESCE(tenant_id, ''), idempotency_key)
    WHERE idempotency_key IS NOT NULL;
