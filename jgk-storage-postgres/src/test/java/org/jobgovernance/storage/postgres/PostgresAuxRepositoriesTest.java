package org.jobgovernance.storage.postgres;

import org.flywaydb.core.Flyway;
import org.jobgovernance.core.api.ExecutionContext;
import org.jobgovernance.core.model.ConcurrencyPolicy;
import org.jobgovernance.core.model.ExecutionMode;
import org.jobgovernance.core.model.JobDefinition;
import org.jobgovernance.core.model.JobDefinitionState;
import org.jobgovernance.core.model.JobPolicy;
import org.jobgovernance.core.model.JobSchedule;
import org.jobgovernance.core.model.MisfirePolicy;
import org.jobgovernance.core.model.TriggerType;
import org.jobgovernance.core.policy.IdempotencyStrategy;
import org.jobgovernance.core.policy.RetryStrategies;
import org.jobgovernance.core.policy.TimeoutPolicy;
import org.jobgovernance.storage.spi.AuditEventRepository;
import org.jobgovernance.storage.spi.ManualTriggerRequestRepository;
import org.jobgovernance.storage.spi.ScheduleCursorRepository;
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
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers(disabledWithoutDocker = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PostgresAuxRepositoriesTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("jgk")
            .withUsername("jgk")
            .withPassword("jgk");

    private DataSource dataSource;
    private PostgresJobDefinitionRepository jobDefinitionRepository;
    private PostgresScheduleCursorRepository scheduleCursorRepository;
    private PostgresManualTriggerRequestRepository manualTriggerRequestRepository;
    private PostgresAuditEventRepository auditEventRepository;

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
        this.jobDefinitionRepository = new PostgresJobDefinitionRepository(dataSource);
        this.scheduleCursorRepository = new PostgresScheduleCursorRepository(dataSource);
        this.manualTriggerRequestRepository = new PostgresManualTriggerRequestRepository(dataSource);
        this.auditEventRepository = new PostgresAuditEventRepository(dataSource);
        truncateAll();
    }

    @Test
    void shouldUpsertAndQueryDefinitionAndScheduleCursor() {
        Instant now = Instant.parse("2026-01-02T00:00:00Z");
        JobDefinition definition = sampleDefinition("billing.reconcile", JobDefinitionState.ENABLED);

        jobDefinitionRepository.upsert(definition, now);

        Optional<JobDefinition> loaded = jobDefinitionRepository.findByJobKey(definition.jobKey());
        assertTrue(loaded.isPresent());
        assertEquals(definition.jobKey(), loaded.get().jobKey());
        assertEquals(JobDefinitionState.ENABLED, loaded.get().state());
        assertEquals(ExecutionMode.BLOCKING, loaded.get().executionMode());
        assertNotNull(loaded.get().policy());
        assertEquals(1, jobDefinitionRepository.findEnabled(10).size());

        ScheduleCursorRepository.ScheduleCursor cursor = scheduleCursorRepository.findByJobKey(definition.jobKey()).orElseThrow();
        assertEquals(0L, cursor.cursorVersion());

        ScheduleCursorRepository.ScheduleCursor updated = new ScheduleCursorRepository.ScheduleCursor(
                cursor.jobKey(),
                now,
                now.plusSeconds(120),
                cursor.cursorVersion()
        );
        scheduleCursorRepository.updateCursor(updated, now.plusSeconds(1));

        ScheduleCursorRepository.ScheduleCursor reloaded = scheduleCursorRepository.findByJobKey(definition.jobKey()).orElseThrow();
        assertEquals(1L, reloaded.cursorVersion());
        assertEquals(now, reloaded.lastEvaluatedAt());
        assertEquals(now.plusSeconds(120), reloaded.nextMaterializeAt());

        assertTrue(jobDefinitionRepository.updateState(definition.jobKey(), JobDefinitionState.PAUSED, "ops", now.plusSeconds(2)));
        assertEquals(0, jobDefinitionRepository.findEnabled(10).size());
    }

    @Test
    void shouldEnqueueFetchAndProcessManualTriggerRequests() throws SQLException {
        Instant now = Instant.parse("2026-01-03T00:00:00Z");
        JobDefinition definition = sampleDefinition("catalog.refresh-cache", JobDefinitionState.ENABLED);
        jobDefinitionRepository.upsert(definition, now);

        ManualTriggerRequestRepository.ManualTriggerRequest first = new ManualTriggerRequestRepository.ManualTriggerRequest(
                UUID.randomUUID(),
                definition.jobKey(),
                null,
                "inline-json:{\"tenant\":\"acme\"}",
                "manual-idem-1",
                now.plusSeconds(10),
                "alice",
                now
        );
        UUID firstId = manualTriggerRequestRepository.enqueue(first);

        ManualTriggerRequestRepository.ManualTriggerRequest duplicate = new ManualTriggerRequestRepository.ManualTriggerRequest(
                UUID.randomUUID(),
                definition.jobKey(),
                null,
                "inline-json:{\"tenant\":\"acme\"}",
                "manual-idem-1",
                now.plusSeconds(10),
                "alice",
                now.plusSeconds(1)
        );
        UUID duplicateId = manualTriggerRequestRepository.enqueue(duplicate);
        assertEquals(firstId, duplicateId);

        List<ManualTriggerRequestRepository.ManualTriggerRequest> pending = manualTriggerRequestRepository.fetchPending(10, now.plusSeconds(20));
        assertEquals(1, pending.size());
        assertEquals(firstId, pending.getFirst().requestId());

        UUID executionId = UUID.randomUUID();
        insertExecution(executionId, definition.jobKey(), now.plusSeconds(10));
        boolean processed = manualTriggerRequestRepository.markProcessed(firstId, now.plusSeconds(30), executionId);
        boolean processedAgain = manualTriggerRequestRepository.markProcessed(firstId, now.plusSeconds(31), executionId);
        assertTrue(processed);
        assertFalse(processedAgain);
        assertTrue(manualTriggerRequestRepository.fetchPending(10, now.plusSeconds(40)).isEmpty());
    }

    @Test
    void shouldAppendAndQueryAuditEvents() throws SQLException {
        Instant now = Instant.parse("2026-01-04T00:00:00Z");
        JobDefinition definition = sampleDefinition("inventory.rebuild", JobDefinitionState.ENABLED);
        jobDefinitionRepository.upsert(definition, now);

        UUID executionId = UUID.randomUUID();
        insertExecution(executionId, definition.jobKey(), now.plusSeconds(1));

        auditEventRepository.append(new AuditEventRepository.AuditEvent(
                UUID.randomUUID(),
                "JOB_PAUSED",
                definition.jobKey(),
                null,
                "ops",
                "{\"reason\":\"manual\"}",
                now.plusSeconds(2)
        ));
        auditEventRepository.append(new AuditEventRepository.AuditEvent(
                UUID.randomUUID(),
                "EXECUTION_CANCELLED",
                definition.jobKey(),
                executionId,
                "ops",
                "{\"reason\":\"request\"}",
                now.plusSeconds(3)
        ));

        List<AuditEventRepository.AuditEvent> byJob = auditEventRepository.findByJobKey(definition.jobKey(), 10);
        List<AuditEventRepository.AuditEvent> byExecution = auditEventRepository.findByExecutionId(executionId, 10);

        assertEquals(2, byJob.size());
        assertEquals("EXECUTION_CANCELLED", byJob.getFirst().eventType());
        assertEquals(1, byExecution.size());
        assertEquals(executionId, byExecution.getFirst().executionId());
    }

    private JobDefinition sampleDefinition(String jobKey, JobDefinitionState state) {
        return new JobDefinition(
                jobKey,
                1,
                jobKey,
                "integration test definition",
                "team-a",
                Set.of("integration", "postgres"),
                ExecutionMode.BLOCKING,
                new JobSchedule.FixedDelaySchedule(Duration.ofMinutes(5), Duration.ofSeconds(30), ZoneId.of("UTC"), null, null),
                new JobPolicy(
                        RetryStrategies.fixedDelay(new RetryStrategies.FixedDelayConfig(4, Duration.ofSeconds(20))),
                        new TimeoutPolicy.DefaultTimeoutPolicy(
                                Duration.ofMinutes(10),
                                Duration.ofMinutes(5),
                                Duration.ofMinutes(7),
                                Duration.ofSeconds(30),
                                Duration.ofMinutes(2)
                        ),
                        MisfirePolicy.CATCH_UP_LATEST_ONLY,
                        new ConcurrencyPolicy.ForbidOverlap(),
                        new NoopIdempotencyStrategy(),
                        true
                ),
                "schema-v1",
                state,
                true,
                false,
                null
        );
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

    private void insertExecution(UUID executionId, String jobKey, Instant scheduledAt) throws SQLException {
        String sql = """
                INSERT INTO job_execution(
                    execution_id,
                    job_key,
                    trigger_type,
                    scheduled_at,
                    claimable_at,
                    status,
                    attempt,
                    max_attempts,
                    cancellation_requested,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, 'SCHEDULED', 1, 3, FALSE, ?, ?)
                """;
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setObject(1, executionId);
            statement.setString(2, jobKey);
            statement.setString(3, TriggerType.MANUAL.name());
            statement.setTimestamp(4, Timestamp.from(scheduledAt));
            statement.setTimestamp(5, Timestamp.from(scheduledAt));
            statement.setTimestamp(6, Timestamp.from(scheduledAt.minusSeconds(1)));
            statement.setTimestamp(7, Timestamp.from(scheduledAt.minusSeconds(1)));
            statement.executeUpdate();
        }
    }

    private static final class NoopIdempotencyStrategy implements IdempotencyStrategy {
        @Override
        public Optional<String> deduplicationKey(ExecutionContext<?> context) {
            return Optional.empty();
        }

        @Override
        public BeforeExecutionDecision beforeExecution(ExecutionContext<?> context, String deduplicationKey) {
            return BeforeExecutionDecision.EXECUTE;
        }

        @Override
        public void afterExecution(ExecutionContext<?> context, String deduplicationKey, AfterExecutionResult result) {
        }
    }
}
