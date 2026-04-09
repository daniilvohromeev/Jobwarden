package org.jobgovernance.spring.core;

import org.jobgovernance.core.api.ExecutionContext;
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
import org.jobgovernance.core.policy.RetryStrategies;
import org.jobgovernance.core.policy.TimeoutPolicy;
import org.jobgovernance.storage.spi.ExecutionRepository;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultJobGovernanceManagementServiceTest {

    @Test
    void shouldTriggerNowAndReturnExecutionView() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        InMemoryJobRegistry registry = new InMemoryJobRegistry();
        registry.register(registration("billing.reconcile", null));
        FakeExecutionRepository repository = new FakeExecutionRepository();
        DefaultJobGovernanceManagementService service = new DefaultJobGovernanceManagementService(
                registry,
                repository,
                Clock.fixed(now, ZoneOffset.UTC)
        );

        JobGovernanceManagementService.ExecutionView view = service.triggerNow(
                "billing.reconcile",
                null,
                "operator",
                "{\"manual\":true}",
                "idem-1"
        );

        assertNotNull(view.executionId());
        assertEquals("billing.reconcile", view.jobKey());
        assertEquals("SCHEDULED", view.status());
        assertEquals(1, repository.executions.size());
        assertEquals(view.executionId(), repository.executions.values().iterator().next().executionId());
    }

    @Test
    void shouldDeduplicateManualTriggerByIdempotencyKey() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        InMemoryJobRegistry registry = new InMemoryJobRegistry();
        registry.register(registration("billing.reconcile", "tenant-a"));
        FakeExecutionRepository repository = new FakeExecutionRepository();
        DefaultJobGovernanceManagementService service = new DefaultJobGovernanceManagementService(
                registry,
                repository,
                Clock.fixed(now, ZoneOffset.UTC)
        );

        JobGovernanceManagementService.ExecutionView first = service.triggerNow(
                "billing.reconcile",
                "tenant-a",
                "operator",
                "{\"manual\":true}",
                "idem-1"
        );
        JobGovernanceManagementService.ExecutionView second = service.triggerNow(
                "billing.reconcile",
                "tenant-a",
                "operator",
                "{\"manual\":true}",
                "idem-1"
        );

        assertEquals(first.executionId(), second.executionId());
        assertEquals(1, repository.executions.size());
    }

    @Test
    void shouldPauseAndResumeJobByUpdatingRegistry() {
        InMemoryJobRegistry registry = new InMemoryJobRegistry();
        registry.register(registration("billing.reconcile", null));
        FakeExecutionRepository repository = new FakeExecutionRepository();
        DefaultJobGovernanceManagementService service = new DefaultJobGovernanceManagementService(
                registry,
                repository,
                Clock.systemUTC()
        );

        boolean paused = service.pauseJob("billing.reconcile", "operator");
        JobDefinition pausedDefinition = registry.findByJobKey("billing.reconcile").orElseThrow().definition();
        boolean resumed = service.resumeJob("billing.reconcile", "operator");
        JobDefinition resumedDefinition = registry.findByJobKey("billing.reconcile").orElseThrow().definition();

        assertTrue(paused);
        assertTrue(resumed);
        assertEquals(JobDefinitionState.PAUSED, pausedDefinition.state());
        assertEquals(JobDefinitionState.ENABLED, resumedDefinition.state());
        assertEquals(3, resumedDefinition.version());
    }

    @Test
    void shouldCancelExecutionViaRepository() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        InMemoryJobRegistry registry = new InMemoryJobRegistry();
        registry.register(registration("billing.reconcile", null));
        FakeExecutionRepository repository = new FakeExecutionRepository();
        DefaultJobGovernanceManagementService service = new DefaultJobGovernanceManagementService(
                registry,
                repository,
                Clock.fixed(now, ZoneOffset.UTC)
        );
        UUID executionId = UUID.randomUUID();
        repository.cancellable.add(executionId);

        boolean cancelled = service.cancelExecution(executionId, "operator", "manual cancel");

        assertTrue(cancelled);
        assertEquals(executionId, repository.lastCancelExecutionId);
        assertEquals("operator", repository.lastCancelActor);
        assertEquals("manual cancel", repository.lastCancelReason);
    }

    @Test
    void shouldListJobsAndExecutionsWithTenantFilter() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        InMemoryJobRegistry registry = new InMemoryJobRegistry();
        registry.register(registration("job-global", null));
        registry.register(registration("job-tenant-a", "tenant-a"));
        registry.register(registration("job-tenant-b", "tenant-b"));
        FakeExecutionRepository repository = new FakeExecutionRepository();
        JobExecution execA = repository.insertForTest("job-global", "tenant-a", now.minusSeconds(10));
        repository.insertForTest("job-global", "tenant-a", now.minusSeconds(20));
        repository.insertForTest("job-global", "tenant-b", now.minusSeconds(30));
        DefaultJobGovernanceManagementService service = new DefaultJobGovernanceManagementService(
                registry,
                repository,
                Clock.fixed(now, ZoneOffset.UTC)
        );

        List<JobGovernanceManagementService.JobView> jobs = service.listJobs("tenant-a", 10);
        List<JobGovernanceManagementService.ExecutionView> executions = service.listExecutions("job-global", "tenant-a", 10);

        assertEquals(List.of("job-global", "job-tenant-a"), jobs.stream().map(JobGovernanceManagementService.JobView::jobKey).toList());
        assertEquals(2, executions.size());
        assertEquals(execA.executionId(), executions.getFirst().executionId());
        assertEquals("job-global", executions.getFirst().jobKey());
    }

    @SuppressWarnings("unchecked")
    private static JobRegistry.JobRegistration<Map<String, Object>, String> registration(String jobKey, String tenantScope) {
        JobDefinition definition = new JobDefinition(
                jobKey,
                1,
                jobKey,
                "test",
                "team",
                Set.of("test"),
                ExecutionMode.BLOCKING,
                new JobSchedule.FixedDelaySchedule(Duration.ofMinutes(1), Duration.ZERO, ZoneId.of("UTC"), null, null),
                new JobPolicy(
                        RetryStrategies.fixedDelay(new RetryStrategies.FixedDelayConfig(5, Duration.ofSeconds(10))),
                        new TimeoutPolicy.DefaultTimeoutPolicy(
                                Duration.ofMinutes(1),
                                Duration.ofMinutes(1),
                                Duration.ofMinutes(1),
                                Duration.ofSeconds(10),
                                Duration.ofSeconds(30)
                        ),
                        MisfirePolicy.CATCH_UP_ALL_MISSED_WINDOWS,
                        new ConcurrencyPolicy.ForbidOverlap(),
                        new IdempotencyStrategy() {
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
                        },
                        true
                ),
                "schema-v1",
                JobDefinitionState.ENABLED,
                true,
                false,
                tenantScope
        );
        return new JobRegistry.JobRegistration<>(
                definition,
                (Class<Map<String, Object>>) (Class<?>) Map.class,
                JobRegistry.HandlerType.SYNC,
                (context, token) -> "ok",
                null
        );
    }

    private static final class FakeExecutionRepository implements ExecutionRepository {
        private final Map<UUID, JobExecution> executions = new HashMap<>();
        private final Map<String, UUID> idemIndex = new HashMap<>();
        private final Set<UUID> cancellable = new java.util.HashSet<>();
        private UUID lastCancelExecutionId;
        private String lastCancelActor;
        private String lastCancelReason;

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
            String idemKey = idemComposite(request.jobKey(), request.tenantId(), request.idempotencyKey());
            if (request.idempotencyKey() != null && idemIndex.containsKey(idemKey)) {
                return false;
            }
            UUID executionId = UUID.randomUUID();
            JobExecution execution = new JobExecution(
                    executionId,
                    request.jobKey(),
                    TriggerType.valueOf(request.triggerType()),
                    request.scheduledAt(),
                    null,
                    null,
                    null,
                    ExecutionStatus.SCHEDULED,
                    null,
                    1,
                    request.payloadRef(),
                    null,
                    null,
                    false,
                    0L,
                    null,
                    request.correlationId(),
                    request.traceId(),
                    request.causationId(),
                    request.parentExecutionId(),
                    request.tenantId()
            );
            executions.put(executionId, execution);
            if (request.idempotencyKey() != null) {
                idemIndex.put(idemKey, executionId);
            }
            return true;
        }

        @Override
        public Optional<JobExecution> findExecution(UUID executionId) {
            return Optional.ofNullable(executions.get(executionId));
        }

        @Override
        public List<JobExecution> findExecutions(String jobKey, String tenantId, int limit) {
            return executions.values().stream()
                    .filter(execution -> execution.jobKey().equals(jobKey))
                    .filter(execution -> tenantId == null || tenantId.equals(execution.tenantId()))
                    .sorted(Comparator.comparing(JobExecution::scheduledAt).reversed())
                    .limit(limit)
                    .toList();
        }

        @Override
        public Optional<JobExecution> findByIdempotencyKey(String jobKey, String tenantId, String idempotencyKey) {
            UUID executionId = idemIndex.get(idemComposite(jobKey, tenantId, idempotencyKey));
            return executionId == null ? Optional.empty() : Optional.ofNullable(executions.get(executionId));
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
        public boolean requestCancellation(UUID executionId, String actor, String reason, Instant requestedAt) {
            this.lastCancelExecutionId = executionId;
            this.lastCancelActor = actor;
            this.lastCancelReason = reason;
            return cancellable.contains(executionId);
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

        private JobExecution insertForTest(String jobKey, String tenantId, Instant scheduledAt) {
            UUID executionId = UUID.randomUUID();
            JobExecution execution = new JobExecution(
                    executionId,
                    jobKey,
                    TriggerType.MANUAL,
                    scheduledAt,
                    null,
                    null,
                    null,
                    ExecutionStatus.SCHEDULED,
                    null,
                    1,
                    null,
                    null,
                    null,
                    false,
                    0L,
                    null,
                    null,
                    null,
                    null,
                    null,
                    tenantId
            );
            executions.put(executionId, execution);
            return execution;
        }

        private static String idemComposite(String jobKey, String tenantId, String idempotencyKey) {
            if (idempotencyKey == null) {
                return null;
            }
            return jobKey + "|" + (tenantId == null ? "" : tenantId) + "|" + idempotencyKey;
        }
    }
}
