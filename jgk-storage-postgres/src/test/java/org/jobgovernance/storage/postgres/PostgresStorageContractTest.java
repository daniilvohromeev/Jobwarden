package org.jobgovernance.storage.postgres;

import org.flywaydb.core.Flyway;
import org.jobgovernance.core.model.JobDefinitionState;
import org.jobgovernance.core.model.JobExecution;
import org.jobgovernance.storage.spi.AuditEventRepository;
import org.jobgovernance.storage.spi.ExecutionRepository;
import org.jobgovernance.storage.spi.JobDefinitionRepository;
import org.jobgovernance.storage.spi.ManualTriggerRequestRepository;
import org.jobgovernance.storage.spi.ScheduleCursorRepository;
import org.jobgovernance.storage.tck.AuditEventRepositoryTck;
import org.jobgovernance.storage.tck.ExecutionRepositoryTck;
import org.jobgovernance.storage.tck.JobDefinitionRepositoryTck;
import org.jobgovernance.storage.tck.ManualTriggerRequestRepositoryTck;
import org.jobgovernance.storage.tck.ScheduleCursorRepositoryTck;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.UUID;

@Testcontainers(disabledWithoutDocker = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PostgresStorageContractTest implements
        ExecutionRepositoryTck,
        JobDefinitionRepositoryTck,
        ScheduleCursorRepositoryTck,
        ManualTriggerRequestRepositoryTck,
        AuditEventRepositoryTck {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("jgk")
            .withUsername("jgk")
            .withPassword("jgk");

    private DataSource dataSource;
    private PostgresExecutionRepository executionRepository;
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
    void setUpRepositories() {
        this.executionRepository = new PostgresExecutionRepository(dataSource);
        this.jobDefinitionRepository = new PostgresJobDefinitionRepository(dataSource);
        this.scheduleCursorRepository = new PostgresScheduleCursorRepository(dataSource);
        this.manualTriggerRequestRepository = new PostgresManualTriggerRequestRepository(dataSource);
        this.auditEventRepository = new PostgresAuditEventRepository(dataSource);
    }

    @Override
    public ExecutionRepository executionRepository() {
        return executionRepository;
    }

    @Override
    public JobDefinitionRepository jobDefinitionRepository() {
        return jobDefinitionRepository;
    }

    @Override
    public ScheduleCursorRepository scheduleCursorRepository() {
        return scheduleCursorRepository;
    }

    @Override
    public ManualTriggerRequestRepository manualTriggerRequestRepository() {
        return manualTriggerRequestRepository;
    }

    @Override
    public AuditEventRepository auditEventRepository() {
        return auditEventRepository;
    }

    @Override
    public void clearStorage() {
        truncateAll();
    }

    @Override
    public void seedDefinition(String jobKey) {
        jobDefinitionRepository.upsert(sampleDefinition(jobKey, JobDefinitionState.ENABLED), fixedNow());
    }

    @Override
    public UUID seedScheduledExecution(String jobKey, Instant scheduledAt) {
        if (jobDefinitionRepository.findByJobKey(jobKey).isEmpty()) {
            seedDefinition(jobKey);
        }
        String idempotencyKey = "seed-" + UUID.randomUUID();
        executionRepository.enqueueScheduledExecution(
                new ExecutionRepository.ScheduledExecutionInsert(
                        jobKey,
                        null,
                        "MANUAL",
                        scheduledAt,
                        scheduledAt,
                        3,
                        null,
                        "dedupe-" + idempotencyKey,
                        "corr-" + idempotencyKey,
                        null,
                        null,
                        null,
                        idempotencyKey,
                        null
                ),
                fixedNow()
        );
        JobExecution execution = executionRepository.findByIdempotencyKey(jobKey, null, idempotencyKey)
                .orElseThrow(() -> new IllegalStateException("Seed execution was not persisted"));
        return execution.executionId();
    }

    private void truncateAll() {
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
        } catch (SQLException exception) {
            throw new IllegalStateException("Failed to truncate storage tables for contract tests", exception);
        }
    }
}
