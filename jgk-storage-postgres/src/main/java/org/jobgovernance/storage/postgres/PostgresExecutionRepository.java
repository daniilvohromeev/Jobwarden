package org.jobgovernance.storage.postgres;

import org.jobgovernance.core.model.ExecutionStatus;
import org.jobgovernance.core.model.JobExecution;
import org.jobgovernance.core.model.TriggerType;
import org.jobgovernance.storage.spi.ExecutionRepository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

public class PostgresExecutionRepository implements ExecutionRepository {

    private final DataSource dataSource;

    public PostgresExecutionRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public List<UUID> findDueExecutionIds(Instant now, int batchSize) {
        String sql = """
                SELECT execution_id
                FROM job_execution
                WHERE status = 'SCHEDULED'
                  AND claimable_at <= ?
                  AND cancellation_requested = FALSE
                ORDER BY scheduled_at, execution_id
                LIMIT ?
                """;
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setTimestamp(1, toTimestamp(now));
            statement.setInt(2, batchSize);
            List<UUID> ids = new ArrayList<>(batchSize);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    ids.add(resultSet.getObject(1, UUID.class));
                }
            }
            return ids;
        } catch (SQLException exception) {
            throw repositoryException("find due execution ids", exception);
        }
    }

    @Override
    public Optional<JobExecution> claimExecution(UUID executionId, ClaimRequest request) {
        String sql = """
                UPDATE job_execution
                SET status = 'CLAIMED',
                    claimed_at = ?,
                    worker_id = ?,
                    lease_token = ?,
                    lease_expires_at = ?,
                    fencing_token = fencing_token + 1,
                    updated_at = ?,
                    version = version + 1
                FROM LATERAL (
                    SELECT
                        COALESCE(NULLIF(d.policy_json #>> '{concurrency,kind}', ''), 'FORBID_OVERLAP') AS concurrency_kind,
                        COALESCE(NULLIF(d.policy_json #>> '{concurrency,limit}', '')::INT, 1) AS concurrency_limit
                    FROM job_definition d
                    WHERE d.job_key = job_execution.job_key
                ) policy
                WHERE execution_id = ?
                  AND status = 'SCHEDULED'
                  AND cancellation_requested = FALSE
                  AND claimable_at <= ?
                  AND (
                      policy.concurrency_kind = 'ALLOW_OVERLAP'
                      OR policy.concurrency_kind = 'SHARD_BY_PARTITION_KEY'
                      OR (
                          policy.concurrency_kind IN ('FORBID_OVERLAP', 'SINGLETON_CLUSTER_WIDE')
                          AND NOT EXISTS (
                              SELECT 1
                              FROM job_execution active
                              WHERE active.job_key = job_execution.job_key
                                AND active.execution_id <> job_execution.execution_id
                                AND active.status IN ('CLAIMED', 'RUNNING', 'CANCEL_REQUESTED')
                                AND active.lease_expires_at IS NOT NULL
                                AND active.lease_expires_at > ?
                          )
                      )
                      OR (
                          policy.concurrency_kind = 'SINGLETON_PER_TENANT'
                          AND NOT EXISTS (
                              SELECT 1
                              FROM job_execution active
                              WHERE active.job_key = job_execution.job_key
                                AND active.execution_id <> job_execution.execution_id
                                AND active.status IN ('CLAIMED', 'RUNNING', 'CANCEL_REQUESTED')
                                AND active.lease_expires_at IS NOT NULL
                                AND active.lease_expires_at > ?
                                AND (
                                    (active.tenant_id IS NULL AND job_execution.tenant_id IS NULL)
                                    OR active.tenant_id = job_execution.tenant_id
                                )
                          )
                      )
                      OR (
                          policy.concurrency_kind = 'ALLOW_OVERLAP_UP_TO'
                          AND (
                              SELECT COUNT(*)
                              FROM job_execution active
                              WHERE active.job_key = job_execution.job_key
                                AND active.execution_id <> job_execution.execution_id
                                AND active.status IN ('CLAIMED', 'RUNNING', 'CANCEL_REQUESTED')
                                AND active.lease_expires_at IS NOT NULL
                                AND active.lease_expires_at > ?
                          ) < GREATEST(1, policy.concurrency_limit)
                      )
                  )
                RETURNING *
                """;
        try (Connection connection = dataSource.getConnection()) {
            boolean previousAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setTimestamp(1, toTimestamp(request.claimedAt()));
                statement.setString(2, request.workerId());
                statement.setString(3, request.leaseToken());
                statement.setTimestamp(4, toTimestamp(request.leaseExpiresAt()));
                statement.setTimestamp(5, toTimestamp(request.claimedAt()));
                statement.setObject(6, executionId);
                statement.setTimestamp(7, toTimestamp(request.claimedAt()));
                statement.setTimestamp(8, toTimestamp(request.claimedAt()));
                statement.setTimestamp(9, toTimestamp(request.claimedAt()));
                statement.setTimestamp(10, toTimestamp(request.claimedAt()));
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (!resultSet.next()) {
                        connection.rollback();
                        return Optional.empty();
                    }
                    JobExecution execution = mapExecution(resultSet);
                    upsertExecutionAttemptClaim(connection, execution, request.claimedAt());
                    connection.commit();
                    return Optional.of(execution);
                }
            } catch (SQLException exception) {
                connection.rollback();
                throw exception;
            } finally {
                connection.setAutoCommit(previousAutoCommit);
            }
        } catch (SQLException exception) {
            throw repositoryException("claim execution", exception);
        }
    }

    @Override
    public boolean enqueueScheduledExecution(ScheduledExecutionInsert request, Instant createdAt) {
        String sql = """
                INSERT INTO job_execution(
                    job_key,
                    tenant_id,
                    trigger_type,
                    scheduled_at,
                    claimable_at,
                    status,
                    attempt,
                    max_attempts,
                    payload_ref,
                    dedupe_key,
                    correlation_id,
                    trace_id,
                    causation_id,
                    parent_execution_id,
                    idempotency_key,
                    business_key,
                    cancellation_requested,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, 'SCHEDULED', 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, FALSE, ?, ?)
                ON CONFLICT DO NOTHING
                """;
        return executeUpdate(sql, statement -> {
            statement.setString(1, request.jobKey());
            statement.setString(2, request.tenantId());
            statement.setString(3, request.triggerType());
            statement.setTimestamp(4, toTimestamp(request.scheduledAt()));
            statement.setTimestamp(5, toTimestamp(request.claimableAt()));
            statement.setInt(6, request.maxAttempts());
            statement.setString(7, request.payloadRef());
            statement.setString(8, request.dedupeKey());
            statement.setString(9, request.correlationId());
            statement.setString(10, request.traceId());
            statement.setString(11, request.causationId());
            statement.setObject(12, request.parentExecutionId());
            statement.setString(13, request.idempotencyKey());
            statement.setString(14, request.businessKey());
            statement.setTimestamp(15, toTimestamp(createdAt));
            statement.setTimestamp(16, toTimestamp(createdAt));
        }) > 0;
    }

    @Override
    public Optional<JobExecution> findExecution(UUID executionId) {
        String sql = """
                SELECT *
                FROM job_execution
                WHERE execution_id = ?
                """;
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setObject(1, executionId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }
                return Optional.of(mapExecution(resultSet));
            }
        } catch (SQLException exception) {
            throw repositoryException("find execution", exception);
        }
    }

    @Override
    public List<JobExecution> findExecutions(String jobKey, String tenantId, int limit) {
        String sql = """
                SELECT *
                FROM job_execution
                WHERE job_key = ?
                  AND (? IS NULL OR tenant_id = ?)
                ORDER BY scheduled_at DESC, execution_id DESC
                LIMIT ?
                """;
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, jobKey);
            statement.setString(2, tenantId);
            statement.setString(3, tenantId);
            statement.setInt(4, limit);
            List<JobExecution> executions = new ArrayList<>(Math.min(limit, 128));
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    executions.add(mapExecution(resultSet));
                }
            }
            return executions;
        } catch (SQLException exception) {
            throw repositoryException("find executions", exception);
        }
    }

    @Override
    public Optional<JobExecution> findByIdempotencyKey(String jobKey, String tenantId, String idempotencyKey) {
        String sql = """
                SELECT *
                FROM job_execution
                WHERE job_key = ?
                  AND (? IS NULL OR tenant_id = ?)
                  AND idempotency_key = ?
                ORDER BY created_at DESC
                LIMIT 1
                """;
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, jobKey);
            statement.setString(2, tenantId);
            statement.setString(3, tenantId);
            statement.setString(4, idempotencyKey);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }
                return Optional.of(mapExecution(resultSet));
            }
        } catch (SQLException exception) {
            throw repositoryException("find by idempotency key", exception);
        }
    }

    @Override
    public List<WorkerStatus> findActiveWorkers(Instant now, int limit) {
        String sql = """
                SELECT
                    worker_id,
                    COUNT(*) AS active_executions,
                    MIN(claimed_at) AS oldest_claimed_at,
                    MAX(last_heartbeat_at) AS last_heartbeat_at,
                    MAX(lease_expires_at) AS lease_expires_at
                FROM job_execution
                WHERE status IN ('CLAIMED', 'RUNNING', 'CANCEL_REQUESTED')
                  AND worker_id IS NOT NULL
                  AND lease_expires_at IS NOT NULL
                  AND lease_expires_at > ?
                GROUP BY worker_id
                ORDER BY active_executions DESC, worker_id
                LIMIT ?
                """;
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setTimestamp(1, toTimestamp(now));
            statement.setInt(2, limit);
            List<WorkerStatus> workers = new ArrayList<>(Math.min(limit, 64));
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    workers.add(new WorkerStatus(
                            resultSet.getString("worker_id"),
                            resultSet.getInt("active_executions"),
                            toInstant(resultSet, "oldest_claimed_at"),
                            toInstant(resultSet, "last_heartbeat_at"),
                            toInstant(resultSet, "lease_expires_at")
                    ));
                }
            }
            return workers;
        } catch (SQLException exception) {
            throw repositoryException("find active workers", exception);
        }
    }

    @Override
    public boolean markRunning(UUID executionId, String workerId, String leaseToken, Instant startedAt) {
        String sql = """
                UPDATE job_execution
                SET status = 'RUNNING',
                    started_at = ?,
                    updated_at = ?,
                    version = version + 1
                WHERE execution_id = ?
                  AND status = 'CLAIMED'
                  AND worker_id = ?
                  AND lease_token = ?
                """;
        int updated = executeUpdate(sql, statement -> {
            statement.setTimestamp(1, toTimestamp(startedAt));
            statement.setTimestamp(2, toTimestamp(startedAt));
            statement.setObject(3, executionId);
            statement.setString(4, workerId);
            statement.setString(5, leaseToken);
        });
        if (updated > 0) {
            updateCurrentAttempt(executionId, startedAt, null, "RUNNING", null, null);
            return true;
        }
        return false;
    }

    @Override
    public boolean markSucceeded(UUID executionId, String workerId, String leaseToken, String resultSummary, Instant finishedAt) {
        String sql = """
                UPDATE job_execution
                SET status = 'SUCCEEDED',
                    finished_at = ?,
                    result_summary = ?,
                    updated_at = ?,
                    version = version + 1
                WHERE execution_id = ?
                  AND status IN ('CLAIMED', 'RUNNING', 'CANCEL_REQUESTED')
                  AND worker_id = ?
                  AND lease_token = ?
                """;
        int updated = executeUpdate(sql, statement -> {
            statement.setTimestamp(1, toTimestamp(finishedAt));
            statement.setString(2, resultSummary);
            statement.setTimestamp(3, toTimestamp(finishedAt));
            statement.setObject(4, executionId);
            statement.setString(5, workerId);
            statement.setString(6, leaseToken);
        });
        if (updated > 0) {
            updateCurrentAttempt(executionId, null, finishedAt, "SUCCEEDED", null, null);
            return true;
        }
        return false;
    }

    @Override
    public boolean markFailedRetryable(
            UUID executionId,
            String workerId,
            String leaseToken,
            String errorClass,
            String errorSummary,
            Instant nextRetryAt,
            Instant finishedAt
    ) {
        String sql = """
                UPDATE job_execution
                SET status = 'FAILED_RETRYABLE',
                    finished_at = ?,
                    retry_after = ?,
                    error_class = ?,
                    error_summary = ?,
                    updated_at = ?,
                    version = version + 1
                WHERE execution_id = ?
                  AND status IN ('CLAIMED', 'RUNNING', 'CANCEL_REQUESTED')
                  AND worker_id = ?
                  AND lease_token = ?
                """;
        int updated = executeUpdate(sql, statement -> {
            statement.setTimestamp(1, toTimestamp(finishedAt));
            statement.setTimestamp(2, toTimestamp(nextRetryAt));
            statement.setString(3, errorClass);
            statement.setString(4, errorSummary);
            statement.setTimestamp(5, toTimestamp(finishedAt));
            statement.setObject(6, executionId);
            statement.setString(7, workerId);
            statement.setString(8, leaseToken);
        });
        if (updated > 0) {
            updateCurrentAttempt(executionId, null, finishedAt, "FAILED_RETRYABLE", errorClass, errorSummary);
            return true;
        }
        return false;
    }

    @Override
    public boolean markFailedFinal(UUID executionId, String workerId, String leaseToken, String errorClass, String errorSummary, Instant finishedAt) {
        String sql = """
                UPDATE job_execution
                SET status = 'FAILED_FINAL',
                    finished_at = ?,
                    error_class = ?,
                    error_summary = ?,
                    updated_at = ?,
                    version = version + 1
                WHERE execution_id = ?
                  AND status IN ('CLAIMED', 'RUNNING', 'CANCEL_REQUESTED')
                  AND worker_id = ?
                  AND lease_token = ?
                """;
        int updated = executeUpdate(sql, statement -> {
            statement.setTimestamp(1, toTimestamp(finishedAt));
            statement.setString(2, errorClass);
            statement.setString(3, errorSummary);
            statement.setTimestamp(4, toTimestamp(finishedAt));
            statement.setObject(5, executionId);
            statement.setString(6, workerId);
            statement.setString(7, leaseToken);
        });
        if (updated > 0) {
            updateCurrentAttempt(executionId, null, finishedAt, "FAILED_FINAL", errorClass, errorSummary);
            return true;
        }
        return false;
    }

    @Override
    public boolean markTimedOut(UUID executionId, String workerId, String leaseToken, String reason, Instant finishedAt) {
        String sql = """
                UPDATE job_execution
                SET status = 'TIMED_OUT',
                    finished_at = ?,
                    error_class = 'TimeoutException',
                    error_summary = ?,
                    updated_at = ?,
                    version = version + 1
                WHERE execution_id = ?
                  AND status IN ('CLAIMED', 'RUNNING', 'CANCEL_REQUESTED')
                  AND worker_id = ?
                  AND lease_token = ?
                """;
        int updated = executeUpdate(sql, statement -> {
            statement.setTimestamp(1, toTimestamp(finishedAt));
            statement.setString(2, reason);
            statement.setTimestamp(3, toTimestamp(finishedAt));
            statement.setObject(4, executionId);
            statement.setString(5, workerId);
            statement.setString(6, leaseToken);
        });
        if (updated > 0) {
            updateCurrentAttempt(executionId, null, finishedAt, "TIMED_OUT", "TimeoutException", reason);
            return true;
        }
        return false;
    }

    @Override
    public boolean markSkipped(UUID executionId, String workerId, String leaseToken, String reason, Instant finishedAt) {
        String sql = """
                UPDATE job_execution
                SET status = 'SKIPPED',
                    finished_at = ?,
                    result_summary = ?,
                    updated_at = ?,
                    version = version + 1
                WHERE execution_id = ?
                  AND status IN ('CLAIMED', 'RUNNING', 'CANCEL_REQUESTED')
                  AND worker_id = ?
                  AND lease_token = ?
                """;
        int updated = executeUpdate(sql, statement -> {
            statement.setTimestamp(1, toTimestamp(finishedAt));
            statement.setString(2, reason);
            statement.setTimestamp(3, toTimestamp(finishedAt));
            statement.setObject(4, executionId);
            statement.setString(5, workerId);
            statement.setString(6, leaseToken);
        });
        if (updated > 0) {
            updateCurrentAttempt(executionId, null, finishedAt, "SKIPPED", null, reason);
            return true;
        }
        return false;
    }

    @Override
    public boolean requestCancellation(UUID executionId, String actor, String reason, Instant requestedAt) {
        String sql = """
                UPDATE job_execution
                SET cancellation_requested = TRUE,
                    cancellation_requested_at = ?,
                    status = CASE
                        WHEN status = 'SCHEDULED' THEN 'CANCELLED'
                        WHEN status IN ('CLAIMED', 'RUNNING') THEN 'CANCEL_REQUESTED'
                        ELSE status
                    END,
                    updated_at = ?,
                    version = version + 1
                WHERE execution_id = ?
                  AND status IN ('SCHEDULED', 'CLAIMED', 'RUNNING')
                """;
        return executeUpdate(sql, statement -> {
            statement.setTimestamp(1, toTimestamp(requestedAt));
            statement.setTimestamp(2, toTimestamp(requestedAt));
            statement.setObject(3, executionId);
        }) > 0;
    }

    @Override
    public boolean markCancelled(UUID executionId, String workerId, String leaseToken, String reason, Instant finishedAt) {
        String sql = """
                UPDATE job_execution
                SET status = 'CANCELLED',
                    finished_at = ?,
                    error_summary = ?,
                    updated_at = ?,
                    version = version + 1
                WHERE execution_id = ?
                  AND status IN ('CLAIMED', 'RUNNING', 'CANCEL_REQUESTED')
                  AND worker_id = ?
                  AND lease_token = ?
                """;
        int updated = executeUpdate(sql, statement -> {
            statement.setTimestamp(1, toTimestamp(finishedAt));
            statement.setString(2, reason);
            statement.setTimestamp(3, toTimestamp(finishedAt));
            statement.setObject(4, executionId);
            statement.setString(5, workerId);
            statement.setString(6, leaseToken);
        });
        if (updated > 0) {
            updateCurrentAttempt(executionId, null, finishedAt, "CANCELLED", null, reason);
            return true;
        }
        return false;
    }

    @Override
    public boolean renewLease(UUID executionId, String workerId, String leaseToken, Instant leaseExpiresAt, Instant heartbeatAt) {
        String sql = """
                UPDATE job_execution
                SET lease_expires_at = ?,
                    last_heartbeat_at = ?,
                    updated_at = ?,
                    version = version + 1
                WHERE execution_id = ?
                  AND status IN ('CLAIMED', 'RUNNING', 'CANCEL_REQUESTED')
                  AND worker_id = ?
                  AND lease_token = ?
                  AND lease_expires_at > ?
                """;
        return executeUpdate(sql, statement -> {
            statement.setTimestamp(1, toTimestamp(leaseExpiresAt));
            statement.setTimestamp(2, toTimestamp(heartbeatAt));
            statement.setTimestamp(3, toTimestamp(heartbeatAt));
            statement.setObject(4, executionId);
            statement.setString(5, workerId);
            statement.setString(6, leaseToken);
            statement.setTimestamp(7, toTimestamp(heartbeatAt));
        }) > 0;
    }

    @Override
    public int requeueRetryableExecutions(Instant retryDueAt, int batchSize, Instant now) {
        String sql = """
                WITH due AS (
                    SELECT execution_id
                    FROM job_execution
                    WHERE status = 'FAILED_RETRYABLE'
                      AND retry_after IS NOT NULL
                      AND retry_after <= ?
                    ORDER BY retry_after, execution_id
                    LIMIT ?
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE job_execution e
                SET status = 'SCHEDULED',
                    claimable_at = COALESCE(e.retry_after, ?),
                    retry_after = NULL,
                    claimed_at = NULL,
                    started_at = NULL,
                    finished_at = NULL,
                    worker_id = NULL,
                    lease_token = NULL,
                    lease_expires_at = NULL,
                    last_heartbeat_at = NULL,
                    cancellation_requested = FALSE,
                    cancellation_requested_at = NULL,
                    attempt = e.attempt + 1,
                    updated_at = ?,
                    version = version + 1
                FROM due
                WHERE e.execution_id = due.execution_id
                """;
        return executeUpdate(sql, statement -> {
            statement.setTimestamp(1, toTimestamp(retryDueAt));
            statement.setInt(2, batchSize);
            statement.setTimestamp(3, toTimestamp(now));
            statement.setTimestamp(4, toTimestamp(now));
        });
    }

    @Override
    public int recoverStaleClaims(Instant leaseExpiredBefore, String recoveryWorkerId, Instant now) {
        String sql = """
                UPDATE job_execution
                SET status = CASE
                    WHEN attempt < max_attempts THEN 'SCHEDULED'
                    ELSE 'DEAD'
                END,
                    worker_id = NULL,
                    lease_token = NULL,
                    lease_expires_at = NULL,
                    claimable_at = ?,
                    updated_at = ?,
                    version = version + 1
                WHERE status IN ('CLAIMED', 'RUNNING', 'CANCEL_REQUESTED')
                  AND lease_expires_at < ?
                """;
        return executeUpdate(sql, statement -> {
            statement.setTimestamp(1, toTimestamp(now));
            statement.setTimestamp(2, toTimestamp(now));
            statement.setTimestamp(3, toTimestamp(leaseExpiredBefore));
        });
    }

    @Override
    public int markDeadExecutions(Instant deadline, String reason, Instant now) {
        String sql = """
                UPDATE job_execution
                SET status = 'DEAD',
                    finished_at = COALESCE(finished_at, ?),
                    error_summary = CASE
                        WHEN error_summary IS NULL OR error_summary = '' THEN ?
                        ELSE LEFT(error_summary || ' | ' || ?, 2048)
                    END,
                    updated_at = ?,
                    version = version + 1
                WHERE status IN ('FAILED_RETRYABLE', 'CLAIMED', 'RUNNING', 'CANCEL_REQUESTED')
                  AND (
                      (status = 'FAILED_RETRYABLE' AND retry_after IS NOT NULL AND retry_after < ?)
                      OR (status IN ('CLAIMED', 'RUNNING', 'CANCEL_REQUESTED') AND lease_expires_at IS NOT NULL AND lease_expires_at < ?)
                  )
                """;
        return executeUpdate(sql, statement -> {
            statement.setTimestamp(1, toTimestamp(now));
            statement.setString(2, reason);
            statement.setString(3, reason);
            statement.setTimestamp(4, toTimestamp(now));
            statement.setTimestamp(5, toTimestamp(deadline));
            statement.setTimestamp(6, toTimestamp(deadline));
        });
    }

    private int executeUpdate(String sql, StatementBinder binder) {
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            binder.bind(statement);
            return statement.executeUpdate();
        } catch (SQLException exception) {
            throw repositoryException("execute update", exception);
        }
    }

    private void upsertExecutionAttemptClaim(Connection connection, JobExecution execution, Instant claimedAt) throws SQLException {
        String sql = """
                INSERT INTO job_execution_attempt(
                    execution_id,
                    attempt_no,
                    worker_id,
                    lease_token,
                    fencing_token,
                    claimed_at,
                    status,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, 'CLAIMED', ?)
                ON CONFLICT (execution_id, attempt_no) DO UPDATE
                SET worker_id = EXCLUDED.worker_id,
                    lease_token = EXCLUDED.lease_token,
                    fencing_token = EXCLUDED.fencing_token,
                    claimed_at = EXCLUDED.claimed_at,
                    status = EXCLUDED.status
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, execution.executionId());
            statement.setInt(2, execution.attempt());
            statement.setString(3, execution.workerId());
            statement.setString(4, execution.leaseToken());
            statement.setLong(5, execution.fencingToken());
            statement.setTimestamp(6, toTimestamp(claimedAt));
            statement.setTimestamp(7, toTimestamp(claimedAt));
            statement.executeUpdate();
        }
    }

    private void updateCurrentAttempt(
            UUID executionId,
            Instant startedAt,
            Instant finishedAt,
            String status,
            String errorClass,
            String errorSummary
    ) {
        String sql = """
                UPDATE job_execution_attempt a
                SET started_at = COALESCE(?, a.started_at),
                    finished_at = COALESCE(?, a.finished_at),
                    status = ?,
                    error_class = ?,
                    error_summary = ?,
                    duration_ms = CASE
                        WHEN COALESCE(?, a.finished_at) IS NOT NULL AND COALESCE(?, a.started_at) IS NOT NULL
                            THEN GREATEST(
                                0,
                                CAST(EXTRACT(EPOCH FROM (COALESCE(?, a.finished_at) - COALESCE(?, a.started_at))) * 1000 AS BIGINT)
                            )
                        ELSE a.duration_ms
                    END
                FROM job_execution e
                WHERE e.execution_id = ?
                  AND a.execution_id = e.execution_id
                  AND a.attempt_no = e.attempt
                """;
        executeUpdate(sql, statement -> {
            statement.setTimestamp(1, toTimestamp(startedAt));
            statement.setTimestamp(2, toTimestamp(finishedAt));
            statement.setString(3, status);
            statement.setString(4, errorClass);
            statement.setString(5, errorSummary);
            statement.setTimestamp(6, toTimestamp(finishedAt));
            statement.setTimestamp(7, toTimestamp(startedAt));
            statement.setTimestamp(8, toTimestamp(finishedAt));
            statement.setTimestamp(9, toTimestamp(startedAt));
            statement.setObject(10, executionId);
        });
    }

    private JobExecution mapExecution(ResultSet resultSet) throws SQLException {
        return new JobExecution(
                resultSet.getObject("execution_id", UUID.class),
                resultSet.getString("job_key"),
                TriggerType.valueOf(resultSet.getString("trigger_type").toUpperCase(Locale.ROOT)),
                toInstant(resultSet, "scheduled_at"),
                toInstant(resultSet, "claimed_at"),
                toInstant(resultSet, "started_at"),
                toInstant(resultSet, "finished_at"),
                ExecutionStatus.valueOf(resultSet.getString("status").toUpperCase(Locale.ROOT)),
                resultSet.getString("worker_id"),
                resultSet.getInt("attempt"),
                resultSet.getString("payload_ref"),
                resultSet.getString("result_summary"),
                resultSet.getString("error_summary"),
                resultSet.getBoolean("cancellation_requested"),
                resultSet.getLong("fencing_token"),
                resultSet.getString("lease_token"),
                resultSet.getString("correlation_id"),
                resultSet.getString("trace_id"),
                resultSet.getString("causation_id"),
                resultSet.getObject("parent_execution_id", UUID.class),
                resultSet.getString("tenant_id")
        );
    }

    private RuntimeException repositoryException(String action, SQLException exception) {
        return new IllegalStateException("PostgreSQL execution repository failed to " + action, exception);
    }

    private static Timestamp toTimestamp(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }

    private static Instant toInstant(ResultSet resultSet, String columnName) throws SQLException {
        Timestamp timestamp = resultSet.getTimestamp(columnName);
        return timestamp == null ? null : timestamp.toInstant();
    }

    @FunctionalInterface
    private interface StatementBinder {
        void bind(PreparedStatement statement) throws SQLException;
    }
}
