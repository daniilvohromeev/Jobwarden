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
                WHERE execution_id = ?
                  AND status = 'SCHEDULED'
                  AND cancellation_requested = FALSE
                  AND claimable_at <= ?
                RETURNING *
                """;
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setTimestamp(1, toTimestamp(request.claimedAt()));
            statement.setString(2, request.workerId());
            statement.setString(3, request.leaseToken());
            statement.setTimestamp(4, toTimestamp(request.leaseExpiresAt()));
            statement.setTimestamp(5, toTimestamp(request.claimedAt()));
            statement.setObject(6, executionId);
            statement.setTimestamp(7, toTimestamp(request.claimedAt()));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }
                return Optional.of(mapExecution(resultSet));
            }
        } catch (SQLException exception) {
            throw repositoryException("claim execution", exception);
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
        return executeUpdate(sql, statement -> {
            statement.setTimestamp(1, toTimestamp(startedAt));
            statement.setTimestamp(2, toTimestamp(startedAt));
            statement.setObject(3, executionId);
            statement.setString(4, workerId);
            statement.setString(5, leaseToken);
        }) > 0;
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
        return executeUpdate(sql, statement -> {
            statement.setTimestamp(1, toTimestamp(finishedAt));
            statement.setString(2, resultSummary);
            statement.setTimestamp(3, toTimestamp(finishedAt));
            statement.setObject(4, executionId);
            statement.setString(5, workerId);
            statement.setString(6, leaseToken);
        }) > 0;
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
        return executeUpdate(sql, statement -> {
            statement.setTimestamp(1, toTimestamp(finishedAt));
            statement.setTimestamp(2, toTimestamp(nextRetryAt));
            statement.setString(3, errorClass);
            statement.setString(4, errorSummary);
            statement.setTimestamp(5, toTimestamp(finishedAt));
            statement.setObject(6, executionId);
            statement.setString(7, workerId);
            statement.setString(8, leaseToken);
        }) > 0;
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
        return executeUpdate(sql, statement -> {
            statement.setTimestamp(1, toTimestamp(finishedAt));
            statement.setString(2, errorClass);
            statement.setString(3, errorSummary);
            statement.setTimestamp(4, toTimestamp(finishedAt));
            statement.setObject(5, executionId);
            statement.setString(6, workerId);
            statement.setString(7, leaseToken);
        }) > 0;
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
        return executeUpdate(sql, statement -> {
            statement.setTimestamp(1, toTimestamp(finishedAt));
            statement.setString(2, reason);
            statement.setTimestamp(3, toTimestamp(finishedAt));
            statement.setObject(4, executionId);
            statement.setString(5, workerId);
            statement.setString(6, leaseToken);
        }) > 0;
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
        return executeUpdate(sql, statement -> {
            statement.setTimestamp(1, toTimestamp(finishedAt));
            statement.setString(2, reason);
            statement.setTimestamp(3, toTimestamp(finishedAt));
            statement.setObject(4, executionId);
            statement.setString(5, workerId);
            statement.setString(6, leaseToken);
        }) > 0;
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
