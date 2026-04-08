package org.jobgovernance.storage.postgres;

import org.flywaydb.core.Flyway;
import org.jobgovernance.core.model.ExecutionStatus;
import org.jobgovernance.storage.spi.ExecutionRepository;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers(disabledWithoutDocker = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PostgresExecutionRepositoryTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("jgk")
            .withUsername("jgk")
            .withPassword("jgk");

    private DataSource dataSource;
    private PostgresExecutionRepository repository;

    @BeforeAll
    void setUpContainerAndSchema() {
        PGSimpleDataSource postgresDataSource = new PGSimpleDataSource();
        postgresDataSource.setURL(postgres.getJdbcUrl());
        postgresDataSource.setUser(postgres.getUsername());
        postgresDataSource.setPassword(postgres.getPassword());
        this.dataSource = postgresDataSource;

        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .cleanDisabled(false)
                .load()
                .migrate();
    }

    @BeforeEach
    void setUp() throws SQLException {
        this.repository = new PostgresExecutionRepository(dataSource);
        truncateAll();
        insertDefinition("billing.reconcile");
    }

    @Test
    void postgresShouldStart() {
        assertTrue(postgres.isRunning());
    }

    @Test
    void shouldFindOnlyDueScheduledExecutions() throws SQLException {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        UUID dueExecutionId = UUID.randomUUID();
        UUID futureExecutionId = UUID.randomUUID();
        UUID runningExecutionId = UUID.randomUUID();

        insertExecution(dueExecutionId, "billing.reconcile", "SCHEDULED", null, null, null, now.minusSeconds(30), now.minusSeconds(30), 1, 3, null);
        insertExecution(futureExecutionId, "billing.reconcile", "SCHEDULED", null, null, null, now.plusSeconds(30), now.plusSeconds(30), 1, 3, null);
        insertExecution(runningExecutionId, "billing.reconcile", "RUNNING", "worker-A", "lease-A", now.plusSeconds(60), now.minusSeconds(30), now.minusSeconds(30), 1, 3, null);

        List<UUID> dueIds = repository.findDueExecutionIds(now, 10);

        assertEquals(List.of(dueExecutionId), dueIds);
    }

    @Test
    void shouldClaimExecutionAtomically() throws SQLException {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        UUID executionId = UUID.randomUUID();
        insertExecution(executionId, "billing.reconcile", "SCHEDULED", null, null, null, now.minusSeconds(5), now.minusSeconds(5), 1, 3, null);

        ExecutionRepository.ClaimRequest claimRequest = new ExecutionRepository.ClaimRequest(
                "worker-1",
                now,
                now.plusSeconds(45),
                "lease-token-1"
        );

        var claimed = repository.claimExecution(executionId, claimRequest);
        var secondClaim = repository.claimExecution(
                executionId,
                new ExecutionRepository.ClaimRequest("worker-2", now, now.plusSeconds(45), "lease-token-2")
        );

        assertTrue(claimed.isPresent());
        assertEquals(ExecutionStatus.CLAIMED, claimed.get().status());
        assertEquals("worker-1", claimed.get().workerId());
        assertEquals("lease-token-1", claimed.get().leaseToken());
        assertFalse(secondClaim.isPresent());
        assertEquals("CLAIMED", status(executionId));
        assertEquals(1L, fencingToken(executionId));
    }

    @Test
    void shouldEnqueueScheduledExecutionAndIgnoreConflicts() throws SQLException {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        Instant scheduledAt = now.plusSeconds(60);
        ExecutionRepository.ScheduledExecutionInsert request = new ExecutionRepository.ScheduledExecutionInsert(
                "billing.reconcile",
                null,
                "CRON",
                scheduledAt,
                scheduledAt,
                5,
                "payload://ref",
                "dedupe-1",
                "corr-1",
                "trace-1",
                "cause-1",
                null,
                "idempotency-1",
                "business-1"
        );

        boolean inserted = repository.enqueueScheduledExecution(request, now);
        boolean duplicate = repository.enqueueScheduledExecution(request, now.plusSeconds(1));

        assertTrue(inserted);
        assertFalse(duplicate);
        assertEquals(1, executionCount("billing.reconcile", scheduledAt));
        assertEquals(5, maxAttempts("billing.reconcile", scheduledAt));
    }

    @Test
    void shouldTransitionRunningAndSucceededWithLeaseGuard() throws SQLException {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        UUID executionId = UUID.randomUUID();
        insertExecution(executionId, "billing.reconcile", "CLAIMED", "worker-1", "lease-1", now.plusSeconds(90), now.minusSeconds(1), now.minusSeconds(1), 1, 3, null);

        boolean markedRunning = repository.markRunning(executionId, "worker-1", "lease-1", now);
        boolean wrongLeaseSuccess = repository.markSucceeded(executionId, "worker-1", "lease-wrong", "ok", now.plusSeconds(2));
        boolean markedSucceeded = repository.markSucceeded(executionId, "worker-1", "lease-1", "ok", now.plusSeconds(2));

        assertTrue(markedRunning);
        assertFalse(wrongLeaseSuccess);
        assertTrue(markedSucceeded);
        assertEquals("SUCCEEDED", status(executionId));
        assertNotNull(finishedAt(executionId));
    }

    @Test
    void shouldRenewLeaseOnlyForOwnerAndMatchingToken() throws SQLException {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        UUID executionId = UUID.randomUUID();
        insertExecution(executionId, "billing.reconcile", "RUNNING", "worker-1", "lease-1", now.plusSeconds(20), now.minusSeconds(10), now.minusSeconds(10), 1, 3, null);

        boolean wrongWorkerRenew = repository.renewLease(executionId, "worker-2", "lease-1", now.plusSeconds(100), now);
        boolean renewed = repository.renewLease(executionId, "worker-1", "lease-1", now.plusSeconds(100), now);

        assertFalse(wrongWorkerRenew);
        assertTrue(renewed);
        assertEquals(now.plusSeconds(100), leaseExpiresAt(executionId));
    }

    @Test
    void shouldHandleCancellationStates() throws SQLException {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        UUID scheduledExecution = UUID.randomUUID();
        UUID cancelRequestedExecution = UUID.randomUUID();

        insertExecution(scheduledExecution, "billing.reconcile", "SCHEDULED", null, null, null, now.minusSeconds(1), now.minusSeconds(1), 1, 3, null);
        insertExecution(cancelRequestedExecution, "billing.reconcile", "CANCEL_REQUESTED", "worker-1", "lease-1", now.plusSeconds(60), now.minusSeconds(1), now.minusSeconds(1), 1, 3, null);

        boolean requested = repository.requestCancellation(scheduledExecution, "admin", "manual cancel", now);
        boolean cancelled = repository.markCancelled(cancelRequestedExecution, "worker-1", "lease-1", "cancelled by operator", now.plusSeconds(5));

        assertTrue(requested);
        assertTrue(cancelled);
        assertEquals("CANCELLED", status(scheduledExecution));
        assertEquals("CANCELLED", status(cancelRequestedExecution));
        assertTrue(cancellationRequested(scheduledExecution));
    }

    @Test
    void shouldRecoverStaleClaimsAndMarkDeadByPolicies() throws SQLException {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        UUID retryableExecution = UUID.randomUUID();
        UUID staleRecoverableExecution = UUID.randomUUID();
        UUID staleDeadExecution = UUID.randomUUID();

        insertExecution(retryableExecution, "billing.reconcile", "FAILED_RETRYABLE", null, null, null, now.minusSeconds(10), now.minusSeconds(10), 1, 3, now.minusSeconds(1));
        insertExecution(staleRecoverableExecution, "billing.reconcile", "RUNNING", "worker-1", "lease-1", now.minusSeconds(1), now.minusSeconds(10), now.minusSeconds(10), 1, 3, null);
        insertExecution(staleDeadExecution, "billing.reconcile", "RUNNING", "worker-2", "lease-2", now.minusSeconds(1), now.minusSeconds(10), now.minusSeconds(10), 3, 3, null);

        int recovered = repository.recoverStaleClaims(now, "recovery-worker", now);
        int markedDead = repository.markDeadExecutions(now, "retention cutoff", now.plusSeconds(5));

        assertEquals(2, recovered);
        assertEquals("SCHEDULED", status(staleRecoverableExecution));
        assertEquals("DEAD", status(staleDeadExecution));
        assertEquals(1, markedDead);
        assertEquals("DEAD", status(retryableExecution));
    }

    @Test
    void shouldRequeueDueRetryableExecutions() throws SQLException {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        UUID dueRetryExecution = UUID.randomUUID();
        UUID futureRetryExecution = UUID.randomUUID();

        insertExecution(
                dueRetryExecution,
                "billing.reconcile",
                "FAILED_RETRYABLE",
                null,
                null,
                null,
                now.minusSeconds(10),
                now.minusSeconds(10),
                1,
                3,
                now.minusSeconds(1)
        );
        insertExecution(
                futureRetryExecution,
                "billing.reconcile",
                "FAILED_RETRYABLE",
                null,
                null,
                null,
                now.minusSeconds(10),
                now.minusSeconds(10),
                1,
                3,
                now.plusSeconds(60)
        );

        int requeued = repository.requeueRetryableExecutions(now, 100, now);

        assertEquals(1, requeued);
        assertEquals("SCHEDULED", status(dueRetryExecution));
        assertEquals(2, attempt(dueRetryExecution));
        assertEquals(now.minusSeconds(1), claimableAt(dueRetryExecution));
        assertNull(retryAfter(dueRetryExecution));

        assertEquals("FAILED_RETRYABLE", status(futureRetryExecution));
        assertEquals(1, attempt(futureRetryExecution));
        assertEquals(now.plusSeconds(60), retryAfter(futureRetryExecution));
    }

    private void truncateAll() throws SQLException {
        String sql = """
                TRUNCATE TABLE
                    job_execution_attempt,
                    job_trigger_request,
                    job_audit_event,
                    job_dead_letter,
                    job_execution,
                    job_schedule,
                    job_definition
                RESTART IDENTITY CASCADE
                """;
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.executeUpdate();
        }
    }

    private void insertDefinition(String jobKey) throws SQLException {
        String sql = """
                INSERT INTO job_definition(
                    job_key,
                    version,
                    display_name,
                    description,
                    owner_team,
                    execution_mode,
                    payload_schema_version,
                    state,
                    policy_json,
                    created_at,
                    updated_at,
                    updated_by
                )
                VALUES (?, 1, ?, 'integration test definition', 'team-a', 'BLOCKING', 'v1', 'ENABLED', '{}'::jsonb, ?, ?, 'it')
                ON CONFLICT (job_key) DO NOTHING
                """;
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, jobKey);
            statement.setString(2, jobKey);
            statement.setTimestamp(3, Timestamp.from(now));
            statement.setTimestamp(4, Timestamp.from(now));
            statement.executeUpdate();
        }
    }

    private void insertExecution(
            UUID executionId,
            String jobKey,
            String status,
            String workerId,
            String leaseToken,
            Instant leaseExpiresAt,
            Instant scheduledAt,
            Instant claimableAt,
            int attempt,
            int maxAttempts,
            Instant retryAfter
    ) throws SQLException {
        String sql = """
                INSERT INTO job_execution(
                    execution_id,
                    job_key,
                    trigger_type,
                    scheduled_at,
                    claimable_at,
                    status,
                    worker_id,
                    attempt,
                    max_attempts,
                    lease_token,
                    lease_expires_at,
                    retry_after,
                    cancellation_requested,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, 'CRON', ?, ?, ?, ?, ?, ?, ?, ?, ?, FALSE, ?, ?)
                """;
        Instant createdAt = scheduledAt.minusSeconds(5);
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setObject(1, executionId);
            statement.setString(2, jobKey);
            statement.setTimestamp(3, Timestamp.from(scheduledAt));
            statement.setTimestamp(4, Timestamp.from(claimableAt));
            statement.setString(5, status);
            statement.setString(6, workerId);
            statement.setInt(7, attempt);
            statement.setInt(8, maxAttempts);
            statement.setString(9, leaseToken);
            statement.setTimestamp(10, toTimestamp(leaseExpiresAt));
            statement.setTimestamp(11, toTimestamp(retryAfter));
            statement.setTimestamp(12, Timestamp.from(createdAt));
            statement.setTimestamp(13, Timestamp.from(createdAt));
            statement.executeUpdate();
        }
    }

    private String status(UUID executionId) throws SQLException {
        return queryString("SELECT status FROM job_execution WHERE execution_id = ?", executionId);
    }

    private long fencingToken(UUID executionId) throws SQLException {
        String sql = "SELECT fencing_token FROM job_execution WHERE execution_id = ?";
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setObject(1, executionId);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getLong(1);
            }
        }
    }

    private boolean cancellationRequested(UUID executionId) throws SQLException {
        String sql = "SELECT cancellation_requested FROM job_execution WHERE execution_id = ?";
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setObject(1, executionId);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getBoolean(1);
            }
        }
    }

    private Instant leaseExpiresAt(UUID executionId) throws SQLException {
        String sql = "SELECT lease_expires_at FROM job_execution WHERE execution_id = ?";
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setObject(1, executionId);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                Timestamp timestamp = resultSet.getTimestamp(1);
                return timestamp == null ? null : timestamp.toInstant();
            }
        }
    }

    private Instant finishedAt(UUID executionId) throws SQLException {
        String sql = "SELECT finished_at FROM job_execution WHERE execution_id = ?";
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setObject(1, executionId);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                Timestamp timestamp = resultSet.getTimestamp(1);
                return timestamp == null ? null : timestamp.toInstant();
            }
        }
    }

    private int attempt(UUID executionId) throws SQLException {
        String sql = "SELECT attempt FROM job_execution WHERE execution_id = ?";
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setObject(1, executionId);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getInt(1);
            }
        }
    }

    private Instant claimableAt(UUID executionId) throws SQLException {
        String sql = "SELECT claimable_at FROM job_execution WHERE execution_id = ?";
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setObject(1, executionId);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                Timestamp timestamp = resultSet.getTimestamp(1);
                return timestamp == null ? null : timestamp.toInstant();
            }
        }
    }

    private Instant retryAfter(UUID executionId) throws SQLException {
        String sql = "SELECT retry_after FROM job_execution WHERE execution_id = ?";
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setObject(1, executionId);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                Timestamp timestamp = resultSet.getTimestamp(1);
                return timestamp == null ? null : timestamp.toInstant();
            }
        }
    }

    private String queryString(String sql, UUID executionId) throws SQLException {
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setObject(1, executionId);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getString(1);
            }
        }
    }

    private int executionCount(String jobKey, Instant scheduledAt) throws SQLException {
        String sql = "SELECT COUNT(*) FROM job_execution WHERE job_key = ? AND scheduled_at = ?";
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, jobKey);
            statement.setTimestamp(2, Timestamp.from(scheduledAt));
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getInt(1);
            }
        }
    }

    private int maxAttempts(String jobKey, Instant scheduledAt) throws SQLException {
        String sql = "SELECT max_attempts FROM job_execution WHERE job_key = ? AND scheduled_at = ?";
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, jobKey);
            statement.setTimestamp(2, Timestamp.from(scheduledAt));
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getInt(1);
            }
        }
    }

    private Timestamp toTimestamp(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }
}
