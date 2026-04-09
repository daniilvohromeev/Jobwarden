package org.jobgovernance.executor;

import org.jobgovernance.core.api.InMemoryJobRegistry;
import org.jobgovernance.core.api.JobRegistry;
import org.jobgovernance.core.model.ConcurrencyPolicy;
import org.jobgovernance.core.model.ExecutionMode;
import org.jobgovernance.core.model.ExecutionStatus;
import org.jobgovernance.core.model.JobDefinition;
import org.jobgovernance.core.model.JobDefinitionState;
import org.jobgovernance.core.model.JobExecution;
import org.jobgovernance.core.model.JobPolicy;
import org.jobgovernance.core.model.JobSchedule;
import org.jobgovernance.core.model.MisfirePolicy;
import org.jobgovernance.core.model.TriggerType;
import org.jobgovernance.core.policy.IdempotencyStrategy;
import org.jobgovernance.core.policy.RetryStrategy;
import org.jobgovernance.core.policy.TimeoutPolicy;
import org.jobgovernance.storage.spi.ExecutionRepository;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HandlerRunnerLoopTest {

    private static final Clock TEST_CLOCK = Clock.fixed(
            Instant.parse("2026-01-01T00:00:02Z"),
            ZoneId.of("UTC")
    );

    @Test
    void shouldMarkSucceededForSyncHandler() {
        InMemoryJobRegistry registry = new InMemoryJobRegistry();
        registry.register(syncRegistration("job-a"));
        FakeExecutionRepository repository = new FakeExecutionRepository();
        HandlerRunnerLoop loop = new HandlerRunnerLoop(
                new LinkedBlockingQueue<>(),
                registry,
                repository,
                "worker-a",
                1,
                TEST_CLOCK
        );

        loop.executeClaimed(claimedExecution("job-a"));

        assertTrue(repository.markRunningCalled);
        assertTrue(repository.markSucceededCalled);
        assertFalse(repository.markCancelledCalled);
        assertFalse(repository.markFailedFinalCalled);
        assertEquals("sync-ok", repository.lastResultSummary);
    }

    @Test
    void shouldMarkFailedWhenNoHandlerRegistered() {
        InMemoryJobRegistry registry = new InMemoryJobRegistry();
        FakeExecutionRepository repository = new FakeExecutionRepository();
        HandlerRunnerLoop loop = new HandlerRunnerLoop(
                new LinkedBlockingQueue<>(),
                registry,
                repository,
                "worker-a",
                1,
                TEST_CLOCK
        );

        loop.executeClaimed(claimedExecution("job-missing"));

        assertFalse(repository.markRunningCalled);
        assertFalse(repository.markSucceededCalled);
        assertTrue(repository.markFailedFinalCalled);
    }

    @Test
    void shouldSkipExecutionWhenMarkRunningRejected() {
        InMemoryJobRegistry registry = new InMemoryJobRegistry();
        registry.register(syncRegistration("job-a"));
        FakeExecutionRepository repository = new FakeExecutionRepository();
        repository.markRunningResult = false;
        HandlerRunnerLoop loop = new HandlerRunnerLoop(
                new LinkedBlockingQueue<>(),
                registry,
                repository,
                "worker-a",
                1,
                TEST_CLOCK
        );

        loop.executeClaimed(claimedExecution("job-a"));

        assertTrue(repository.markRunningCalled);
        assertFalse(repository.markSucceededCalled);
        assertFalse(repository.markFailedFinalCalled);
    }

    @Test
    void shouldMarkCancelledWhenCancellationAlreadyRequested() {
        InMemoryJobRegistry registry = new InMemoryJobRegistry();
        registry.register(syncRegistration("job-a"));
        FakeExecutionRepository repository = new FakeExecutionRepository();
        JobExecution claimed = claimedExecution("job-a");
        repository.markCancellationRequested(claimed.executionId(), claimed.jobKey(), claimed.scheduledAt());
        HandlerRunnerLoop loop = new HandlerRunnerLoop(
                new LinkedBlockingQueue<>(),
                registry,
                repository,
                "worker-a",
                1,
                TEST_CLOCK
        );

        loop.executeClaimed(claimed);

        assertTrue(repository.markRunningCalled);
        assertFalse(repository.markSucceededCalled);
        assertTrue(repository.markCancelledCalled);
        assertFalse(repository.markFailedFinalCalled);
    }

    @Test
    void shouldMarkSkippedWhenIdempotencyStrategyRequestsSkip() {
        InMemoryJobRegistry registry = new InMemoryJobRegistry();
        registry.register(syncRegistration("job-a", new IdempotencyStrategy() {
            @Override
            public Optional<String> deduplicationKey(org.jobgovernance.core.api.ExecutionContext<?> context) {
                return Optional.of("job-a|dedupe");
            }

            @Override
            public BeforeExecutionDecision beforeExecution(org.jobgovernance.core.api.ExecutionContext<?> context, String deduplicationKey) {
                return BeforeExecutionDecision.SKIP_ALREADY_PROCESSED;
            }

            @Override
            public void afterExecution(org.jobgovernance.core.api.ExecutionContext<?> context, String deduplicationKey, AfterExecutionResult result) {
            }
        }));
        FakeExecutionRepository repository = new FakeExecutionRepository();
        HandlerRunnerLoop loop = new HandlerRunnerLoop(
                new LinkedBlockingQueue<>(),
                registry,
                repository,
                "worker-a",
                1,
                TEST_CLOCK
        );

        loop.executeClaimed(claimedExecution("job-a"));

        assertTrue(repository.markRunningCalled);
        assertFalse(repository.markSucceededCalled);
        assertFalse(repository.markCancelledCalled);
        assertTrue(repository.markSkippedCalled);
    }

    @SuppressWarnings("unchecked")
    private JobRegistry.JobRegistration<Map<String, Object>, String> syncRegistration(String jobKey) {
        return syncRegistration(jobKey, noopIdempotencyStrategy());
    }

    @SuppressWarnings("unchecked")
    private JobRegistry.JobRegistration<Map<String, Object>, String> syncRegistration(
            String jobKey,
            IdempotencyStrategy idempotencyStrategy
    ) {
        return new JobRegistry.JobRegistration<>(
                definition(jobKey, idempotencyStrategy),
                (Class<Map<String, Object>>) (Class<?>) Map.class,
                JobRegistry.HandlerType.SYNC,
                (context, cancellationToken) -> "sync-ok",
                null
        );
    }

    private JobDefinition definition(String jobKey, IdempotencyStrategy idempotencyStrategy) {
        return new JobDefinition(
                jobKey,
                1,
                jobKey,
                "test",
                "team",
                Set.of("test"),
                ExecutionMode.BLOCKING,
                new JobSchedule.FixedRateSchedule(Duration.ofMinutes(5), Duration.ZERO, ZoneId.of("UTC"), null, null),
                new JobPolicy(
                        (context, failure) -> RetryStrategy.RetryDecision.noRetry("no retry"),
                        new TimeoutPolicy.DefaultTimeoutPolicy(Duration.ofMinutes(1), Duration.ofMinutes(1), Duration.ofMinutes(1), Duration.ofSeconds(10), Duration.ofSeconds(30)),
                        MisfirePolicy.IGNORE,
                        new ConcurrencyPolicy.ForbidOverlap(),
                        idempotencyStrategy,
                        false
                ),
                "schema-v1",
                JobDefinitionState.ENABLED,
                true,
                false,
                null
        );
    }

    private static IdempotencyStrategy noopIdempotencyStrategy() {
        return new IdempotencyStrategy() {
            @Override
            public Optional<String> deduplicationKey(org.jobgovernance.core.api.ExecutionContext<?> context) {
                return Optional.empty();
            }

            @Override
            public BeforeExecutionDecision beforeExecution(org.jobgovernance.core.api.ExecutionContext<?> context, String deduplicationKey) {
                return BeforeExecutionDecision.EXECUTE;
            }

            @Override
            public void afterExecution(org.jobgovernance.core.api.ExecutionContext<?> context, String deduplicationKey, AfterExecutionResult result) {
            }
        };
    }

    private JobExecution claimedExecution(String jobKey) {
        return new JobExecution(
                UUID.randomUUID(),
                jobKey,
                TriggerType.CRON,
                Instant.parse("2026-01-01T00:00:00Z"),
                Instant.parse("2026-01-01T00:00:01Z"),
                null,
                null,
                ExecutionStatus.CLAIMED,
                "worker-a",
                1,
                null,
                null,
                null,
                false,
                1L,
                "lease-a",
                null,
                null,
                null,
                null,
                null
        );
    }

    private static final class FakeExecutionRepository implements ExecutionRepository {

        boolean markRunningCalled;
        boolean markSucceededCalled;
        boolean markCancelledCalled;
        boolean markSkippedCalled;
        boolean markFailedFinalCalled;
        boolean markRunningResult = true;
        String lastResultSummary;
        private final Map<UUID, JobExecution> executionsById = new HashMap<>();
        private final Set<UUID> cancellationRequestedExecutionIds = new HashSet<>();

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
            if (cancellationRequestedExecutionIds.contains(executionId)) {
                JobExecution base = executionsById.get(executionId);
                if (base != null) {
                    return Optional.of(new JobExecution(
                            base.executionId(),
                            base.jobKey(),
                            base.triggerType(),
                            base.scheduledAt(),
                            base.claimedAt(),
                            base.startedAt(),
                            base.finishedAt(),
                            ExecutionStatus.CANCEL_REQUESTED,
                            base.workerId(),
                            base.attempt(),
                            base.payloadRef(),
                            base.resultSummary(),
                            base.errorSummary(),
                            true,
                            base.fencingToken(),
                            base.leaseToken(),
                            base.correlationId(),
                            base.traceId(),
                            base.causationId(),
                            base.parentExecutionId(),
                            base.tenantId()
                    ));
                }
                return Optional.of(new JobExecution(
                        executionId,
                        "job-a",
                        TriggerType.CRON,
                        Instant.parse("2026-01-01T00:00:00Z"),
                        Instant.parse("2026-01-01T00:00:01Z"),
                        Instant.parse("2026-01-01T00:00:02Z"),
                        null,
                        ExecutionStatus.CANCEL_REQUESTED,
                        "worker-a",
                        1,
                        null,
                        null,
                        null,
                        true,
                        1L,
                        "lease-a",
                        null,
                        null,
                        null,
                        null,
                        null
                ));
            }
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
            markRunningCalled = true;
            return markRunningResult;
        }

        @Override
        public boolean markSucceeded(UUID executionId, String workerId, String leaseToken, String resultSummary, Instant finishedAt) {
            markSucceededCalled = true;
            lastResultSummary = resultSummary;
            return true;
        }

        @Override
        public boolean markFailedRetryable(UUID executionId, String workerId, String leaseToken, String errorClass, String errorSummary, Instant nextRetryAt, Instant finishedAt) {
            return false;
        }

        @Override
        public boolean markFailedFinal(UUID executionId, String workerId, String leaseToken, String errorClass, String errorSummary, Instant finishedAt) {
            markFailedFinalCalled = true;
            return true;
        }

        @Override
        public boolean markTimedOut(UUID executionId, String workerId, String leaseToken, String reason, Instant finishedAt) {
            return false;
        }

        @Override
        public boolean markSkipped(UUID executionId, String workerId, String leaseToken, String reason, Instant finishedAt) {
            markSkippedCalled = true;
            return true;
        }

        @Override
        public boolean requestCancellation(UUID executionId, String actor, String reason, Instant requestedAt) {
            return false;
        }

        @Override
        public boolean markCancelled(UUID executionId, String workerId, String leaseToken, String reason, Instant finishedAt) {
            markCancelledCalled = true;
            return true;
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

        void markCancellationRequested(UUID executionId, String jobKey, Instant scheduledAt) {
            cancellationRequestedExecutionIds.add(executionId);
            executionsById.put(executionId, new JobExecution(
                    executionId,
                    jobKey,
                    TriggerType.CRON,
                    scheduledAt,
                    scheduledAt.plusSeconds(1),
                    scheduledAt.plusSeconds(2),
                    null,
                    ExecutionStatus.RUNNING,
                    "worker-a",
                    1,
                    null,
                    null,
                    null,
                    false,
                    1L,
                    "lease-a",
                    null,
                    null,
                    null,
                    null,
                    null
            ));
        }
    }
}
