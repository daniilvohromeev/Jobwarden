-- Fetch due executions (candidate ids only)
SELECT execution_id
FROM job_execution
WHERE status = 'SCHEDULED'
  AND claimable_at <= :db_now
  AND cancellation_requested = FALSE
ORDER BY scheduled_at, execution_id
LIMIT :batch_size;

-- Atomic claim (compare-and-set style)
UPDATE job_execution
SET status = 'CLAIMED',
    claimed_at = :db_now,
    worker_id = :worker_id,
    lease_token = :lease_token,
    lease_expires_at = :lease_expires_at,
    fencing_token = fencing_token + 1,
    updated_at = :db_now,
    version = version + 1
WHERE execution_id = :execution_id
  AND status = 'SCHEDULED'
  AND claimable_at <= :db_now
RETURNING *;

-- Renew lease / heartbeat
UPDATE job_execution
SET lease_expires_at = :lease_expires_at,
    last_heartbeat_at = :db_now,
    updated_at = :db_now,
    version = version + 1
WHERE execution_id = :execution_id
  AND status IN ('CLAIMED', 'RUNNING', 'CANCEL_REQUESTED')
  AND worker_id = :worker_id
  AND lease_token = :lease_token
  AND lease_expires_at > :db_now
RETURNING execution_id;

-- Finish success
UPDATE job_execution
SET status = 'SUCCEEDED',
    finished_at = :db_now,
    result_summary = :result_summary,
    updated_at = :db_now,
    version = version + 1
WHERE execution_id = :execution_id
  AND status IN ('CLAIMED', 'RUNNING', 'CANCEL_REQUESTED')
  AND worker_id = :worker_id
  AND lease_token = :lease_token
RETURNING execution_id;

-- Finish failure (retryable)
UPDATE job_execution
SET status = 'FAILED_RETRYABLE',
    finished_at = :db_now,
    retry_after = :next_retry_at,
    error_class = :error_class,
    error_summary = :error_summary,
    updated_at = :db_now,
    version = version + 1
WHERE execution_id = :execution_id
  AND status IN ('CLAIMED', 'RUNNING', 'CANCEL_REQUESTED')
  AND worker_id = :worker_id
  AND lease_token = :lease_token
RETURNING execution_id;

-- Recover stale claims
UPDATE job_execution
SET status = CASE
                WHEN attempt < max_attempts THEN 'SCHEDULED'
                ELSE 'DEAD'
             END,
    worker_id = NULL,
    lease_token = NULL,
    lease_expires_at = NULL,
    claimable_at = :db_now,
    updated_at = :db_now,
    version = version + 1
WHERE status IN ('CLAIMED', 'RUNNING', 'CANCEL_REQUESTED')
  AND lease_expires_at < :db_now
RETURNING execution_id, status;

-- Audit insert
INSERT INTO job_audit_event(event_type, job_key, execution_id, tenant_id, actor, details_json, created_at)
VALUES (:event_type, :job_key, :execution_id, :tenant_id, :actor, :details_json::jsonb, :db_now);

-- Cleanup succeeded history
DELETE FROM job_execution
WHERE status = 'SUCCEEDED'
  AND finished_at < :cutoff
LIMIT :batch_size;
