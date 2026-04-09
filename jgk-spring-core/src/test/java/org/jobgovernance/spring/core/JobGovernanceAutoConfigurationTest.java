package org.jobgovernance.spring.core;

import org.jobgovernance.core.model.JobExecution;
import org.jobgovernance.storage.postgres.PostgresAuditEventRepository;
import org.jobgovernance.storage.postgres.PostgresExecutionRepository;
import org.jobgovernance.storage.postgres.PostgresJobDefinitionRepository;
import org.jobgovernance.storage.postgres.PostgresManualTriggerRequestRepository;
import org.jobgovernance.storage.postgres.PostgresScheduleCursorRepository;
import org.jobgovernance.storage.spi.AuditEventRepository;
import org.jobgovernance.storage.spi.ExecutionRepository;
import org.jobgovernance.storage.spi.JobDefinitionRepository;
import org.jobgovernance.storage.spi.ManualTriggerRequestRepository;
import org.jobgovernance.storage.spi.ScheduleCursorRepository;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class JobGovernanceAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(JobGovernanceAutoConfiguration.class))
            .withPropertyValues(
                    "jgk.enabled=true",
                    "jgk.auto-start=false",
                    "jgk.worker-id=test-worker"
            )
            .withBean(javax.sql.DataSource.class, PGSimpleDataSource::new);

    @Test
    void shouldAutoCreatePostgresRepositoriesWhenDataSourcePresent() {
        contextRunner.run(context -> {
            assertEquals(1, context.getBeanNamesForType(ExecutionRepository.class).length);
            assertEquals(1, context.getBeanNamesForType(JobDefinitionRepository.class).length);
            assertEquals(1, context.getBeanNamesForType(ScheduleCursorRepository.class).length);
            assertEquals(1, context.getBeanNamesForType(ManualTriggerRequestRepository.class).length);
            assertEquals(1, context.getBeanNamesForType(AuditEventRepository.class).length);

            assertInstanceOf(PostgresExecutionRepository.class, context.getBean(ExecutionRepository.class));
            assertInstanceOf(PostgresJobDefinitionRepository.class, context.getBean(JobDefinitionRepository.class));
            assertInstanceOf(PostgresScheduleCursorRepository.class, context.getBean(ScheduleCursorRepository.class));
            assertInstanceOf(PostgresManualTriggerRequestRepository.class, context.getBean(ManualTriggerRequestRepository.class));
            assertInstanceOf(PostgresAuditEventRepository.class, context.getBean(AuditEventRepository.class));
        });
    }

    @Test
    void shouldBackOffExecutionRepositoryWhenUserProvidesCustomBean() {
        ExecutionRepository customRepository = new NoopExecutionRepository();
        contextRunner.withBean(ExecutionRepository.class, () -> customRepository)
                .run(context -> {
                    assertEquals(1, context.getBeanNamesForType(ExecutionRepository.class).length);
                    assertSame(customRepository, context.getBean(ExecutionRepository.class));
                });
    }

    private static final class NoopExecutionRepository implements ExecutionRepository {
        @Override
        public List<UUID> findDueExecutionIds(Instant now, int batchSize) {
            return List.of();
        }

        @Override
        public Optional<JobExecution> claimExecution(UUID executionId, ClaimRequest request) {
            return Optional.empty();
        }

        @Override
        public boolean enqueueScheduledExecution(ScheduledExecutionInsert request, Instant createdAt) {
            return false;
        }

        @Override
        public Optional<JobExecution> findExecution(UUID executionId) {
            return Optional.empty();
        }

        @Override
        public List<JobExecution> findExecutions(String jobKey, String tenantId, int limit) {
            return List.of();
        }

        @Override
        public Optional<JobExecution> findByIdempotencyKey(String jobKey, String tenantId, String idempotencyKey) {
            return Optional.empty();
        }

        @Override
        public boolean markRunning(UUID executionId, String workerId, String leaseToken, Instant startedAt) {
            return false;
        }

        @Override
        public boolean markSucceeded(UUID executionId, String workerId, String leaseToken, String resultSummary, Instant finishedAt) {
            return false;
        }

        @Override
        public boolean markFailedRetryable(UUID executionId, String workerId, String leaseToken, String errorClass, String errorSummary, Instant nextRetryAt, Instant finishedAt) {
            return false;
        }

        @Override
        public boolean markFailedFinal(UUID executionId, String workerId, String leaseToken, String errorClass, String errorSummary, Instant finishedAt) {
            return false;
        }

        @Override
        public boolean markTimedOut(UUID executionId, String workerId, String leaseToken, String reason, Instant finishedAt) {
            return false;
        }

        @Override
        public boolean markSkipped(UUID executionId, String workerId, String leaseToken, String reason, Instant finishedAt) {
            return false;
        }

        @Override
        public boolean requestCancellation(UUID executionId, String actor, String reason, Instant requestedAt) {
            return false;
        }

        @Override
        public boolean markCancelled(UUID executionId, String workerId, String leaseToken, String reason, Instant finishedAt) {
            return false;
        }

        @Override
        public boolean renewLease(UUID executionId, String workerId, String leaseToken, Instant leaseExpiresAt, Instant heartbeatAt) {
            return false;
        }

        @Override
        public int requeueRetryableExecutions(Instant retryDueAt, int batchSize, Instant now) {
            return 0;
        }

        @Override
        public int recoverStaleClaims(Instant leaseExpiredBefore, String recoveryWorkerId, Instant now) {
            return 0;
        }

        @Override
        public int markDeadExecutions(Instant deadline, String reason, Instant now) {
            return 0;
        }
    }
}
