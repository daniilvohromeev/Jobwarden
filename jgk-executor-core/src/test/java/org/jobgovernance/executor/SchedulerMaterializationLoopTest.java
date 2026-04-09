package org.jobgovernance.executor;

import org.jobgovernance.core.api.ExecutionContext;
import org.jobgovernance.core.api.InMemoryJobRegistry;
import org.jobgovernance.core.api.ScheduleEvaluator;
import org.jobgovernance.core.model.ConcurrencyPolicy;
import org.jobgovernance.core.model.ExecutionMode;
import org.jobgovernance.core.model.JobDefinition;
import org.jobgovernance.core.model.JobDefinitionState;
import org.jobgovernance.core.model.JobPolicy;
import org.jobgovernance.core.model.JobSchedule;
import org.jobgovernance.core.model.MisfirePolicy;
import org.jobgovernance.core.policy.IdempotencyStrategy;
import org.jobgovernance.core.policy.RetryStrategies;
import org.jobgovernance.core.policy.TimeoutPolicy;
import org.jobgovernance.storage.spi.JobDefinitionRepository;
import org.jobgovernance.storage.spi.ScheduleCursorRepository;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchedulerMaterializationLoopTest {

    @Test
    void shouldEnqueueDueCandidatesAndUpdateCursor() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);
        InMemoryJobRegistry registry = new InMemoryJobRegistry();
        registry.register(new org.jobgovernance.core.api.JobRegistry.JobRegistration<>(
                definitionWithFixedDelay("job-scheduler", 5),
                payloadClass(),
                org.jobgovernance.core.api.JobRegistry.HandlerType.SYNC,
                (context, token) -> "ok",
                null
        ));
        FakeScheduleEvaluator evaluator = new FakeScheduleEvaluator();
        evaluator.evaluationResult = new ScheduleEvaluator.EvaluationResult(
                List.of(
                        new ScheduleEvaluator.DueExecutionCandidate("job-scheduler", now.minusSeconds(60), "CRON", "dedupe-1"),
                        new ScheduleEvaluator.DueExecutionCandidate("job-scheduler", now, "CRON", "dedupe-2")
                ),
                now.plusSeconds(60)
        );
        FakeRepository repository = new FakeRepository();
        SchedulerMaterializationLoop loop = new SchedulerMaterializationLoop(
                registry,
                evaluator,
                repository,
                null,
                null,
                10,
                Duration.ofSeconds(5),
                clock
        );

        int inserted = loop.materializeOnce(now);

        assertEquals(2, inserted);
        assertEquals(2, repository.inserts.size());
        assertEquals(5, repository.inserts.get(0).request().maxAttempts());
        assertEquals("dedupe-2", repository.inserts.get(1).request().dedupeKey());

        Map<String, ScheduleEvaluator.ScheduleCursor> cursors = loop.snapshotCursors();
        ScheduleEvaluator.ScheduleCursor cursor = cursors.get("job-scheduler");
        assertNotNull(cursor);
        assertEquals(now, cursor.lastEvaluatedAt());
        assertEquals(now.plusSeconds(60), cursor.nextFireAt());
    }

    @Test
    void shouldUseDefaultMaxAttemptsWhenRetryStrategyDoesNotExposeHint() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        InMemoryJobRegistry registry = new InMemoryJobRegistry();
        registry.register(new org.jobgovernance.core.api.JobRegistry.JobRegistration<>(
                definitionWithLambdaRetry("job-default-attempts"),
                payloadClass(),
                org.jobgovernance.core.api.JobRegistry.HandlerType.SYNC,
                (context, token) -> "ok",
                null
        ));
        FakeScheduleEvaluator evaluator = new FakeScheduleEvaluator();
        evaluator.evaluationResult = new ScheduleEvaluator.EvaluationResult(
                List.of(new ScheduleEvaluator.DueExecutionCandidate("job-default-attempts", now, "CRON", "dedupe-1")),
                now.plusSeconds(60)
        );
        FakeRepository repository = new FakeRepository();
        SchedulerMaterializationLoop loop = new SchedulerMaterializationLoop(
                registry,
                evaluator,
                repository,
                null,
                null,
                10,
                Duration.ofSeconds(5),
                Clock.fixed(now, ZoneOffset.UTC)
        );

        loop.materializeOnce(now);

        assertEquals(1, repository.inserts.size());
        assertEquals(3, repository.inserts.getFirst().request().maxAttempts());
    }

    @Test
    void shouldSkipNonEnabledDefinitions() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        InMemoryJobRegistry registry = new InMemoryJobRegistry();
        registry.register(new org.jobgovernance.core.api.JobRegistry.JobRegistration<>(
                sampleDefinition("job-disabled", JobDefinitionState.PAUSED),
                payloadClass(),
                org.jobgovernance.core.api.JobRegistry.HandlerType.SYNC,
                (context, token) -> "ok",
                null
        ));
        FakeScheduleEvaluator evaluator = new FakeScheduleEvaluator();
        evaluator.evaluationResult = new ScheduleEvaluator.EvaluationResult(
                List.of(new ScheduleEvaluator.DueExecutionCandidate("job-disabled", now, "CRON", "dedupe-1")),
                now.plusSeconds(30)
        );
        FakeRepository repository = new FakeRepository();
        SchedulerMaterializationLoop loop = new SchedulerMaterializationLoop(
                registry,
                evaluator,
                repository,
                null,
                null,
                10,
                Duration.ofSeconds(5),
                Clock.fixed(now, ZoneOffset.UTC)
        );

        int inserted = loop.materializeOnce(now);

        assertEquals(0, inserted);
        assertTrue(repository.inserts.isEmpty());
    }

    @Test
    void shouldPersistDefinitionAndCursorWhenRepositoriesProvided() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        InMemoryJobRegistry registry = new InMemoryJobRegistry();
        registry.register(new org.jobgovernance.core.api.JobRegistry.JobRegistration<>(
                definitionWithFixedDelay("job-persisted", 4),
                payloadClass(),
                org.jobgovernance.core.api.JobRegistry.HandlerType.SYNC,
                (context, token) -> "ok",
                null
        ));
        FakeScheduleEvaluator evaluator = new FakeScheduleEvaluator();
        evaluator.evaluationResult = new ScheduleEvaluator.EvaluationResult(
                List.of(new ScheduleEvaluator.DueExecutionCandidate("job-persisted", now, "CRON", "dedupe-persisted")),
                now.plusSeconds(120)
        );
        FakeRepository repository = new FakeRepository();
        FakeScheduleCursorRepository cursorRepository = new FakeScheduleCursorRepository();
        cursorRepository.cursor = new ScheduleCursorRepository.ScheduleCursor("job-persisted", now.minusSeconds(30), now.minusSeconds(10), 5L);
        FakeJobDefinitionRepository definitionRepository = new FakeJobDefinitionRepository();

        SchedulerMaterializationLoop loop = new SchedulerMaterializationLoop(
                registry,
                evaluator,
                repository,
                cursorRepository,
                definitionRepository,
                10,
                Duration.ofSeconds(5),
                Clock.fixed(now, ZoneOffset.UTC)
        );

        int inserted = loop.materializeOnce(now);

        assertEquals(1, inserted);
        assertEquals(1, definitionRepository.upserts.size());
        assertEquals("job-persisted", definitionRepository.upserts.getFirst().jobKey());
        assertNotNull(cursorRepository.lastUpdatedCursor);
        assertEquals(5L, cursorRepository.lastUpdatedCursor.cursorVersion());
        assertEquals(now.plusSeconds(120), cursorRepository.lastUpdatedCursor.nextMaterializeAt());
    }

    private static JobDefinition definitionWithFixedDelay(String jobKey, int maxAttempts) {
        return new JobDefinition(
                jobKey,
                1,
                jobKey,
                "test",
                "team",
                Set.of("test"),
                ExecutionMode.BLOCKING,
                new JobSchedule.FixedDelaySchedule(Duration.ofMinutes(1), Duration.ZERO, ZoneId.of("UTC"), null, null),
                new JobPolicy(
                        RetryStrategies.fixedDelay(new RetryStrategies.FixedDelayConfig(maxAttempts, Duration.ofSeconds(15))),
                        new TimeoutPolicy.DefaultTimeoutPolicy(Duration.ofMinutes(1), Duration.ofMinutes(1), Duration.ofMinutes(1), Duration.ofSeconds(10), Duration.ofSeconds(30)),
                        MisfirePolicy.CATCH_UP_ALL_MISSED_WINDOWS,
                        new ConcurrencyPolicy.ForbidOverlap(),
                        new NoopIdempotencyStrategy(),
                        true
                ),
                "schema-v1",
                JobDefinitionState.ENABLED,
                true,
                false,
                null
        );
    }

    @SuppressWarnings("unchecked")
    private static Class<Map<String, Object>> payloadClass() {
        return (Class<Map<String, Object>>) (Class<?>) Map.class;
    }

    private static JobDefinition definitionWithLambdaRetry(String jobKey) {
        return new JobDefinition(
                jobKey,
                1,
                jobKey,
                "test",
                "team",
                Set.of("test"),
                ExecutionMode.BLOCKING,
                new JobSchedule.FixedDelaySchedule(Duration.ofMinutes(1), Duration.ZERO, ZoneId.of("UTC"), null, null),
                new JobPolicy(
                        (context, failure) -> org.jobgovernance.core.policy.RetryStrategy.RetryDecision.noRetry("no retry"),
                        new TimeoutPolicy.DefaultTimeoutPolicy(Duration.ofMinutes(1), Duration.ofMinutes(1), Duration.ofMinutes(1), Duration.ofSeconds(10), Duration.ofSeconds(30)),
                        MisfirePolicy.CATCH_UP_ALL_MISSED_WINDOWS,
                        new ConcurrencyPolicy.ForbidOverlap(),
                        new NoopIdempotencyStrategy(),
                        true
                ),
                "schema-v1",
                JobDefinitionState.ENABLED,
                true,
                false,
                null
        );
    }

    private static JobDefinition sampleDefinition(String jobKey, JobDefinitionState state) {
        JobDefinition base = definitionWithFixedDelay(jobKey, 3);
        return new JobDefinition(
                base.jobKey(),
                base.version(),
                base.displayName(),
                base.description(),
                base.ownerTeam(),
                base.tags(),
                base.executionMode(),
                base.schedule(),
                base.policy(),
                base.payloadSchemaVersion(),
                state,
                base.manualTriggerable(),
                base.internalOnly(),
                base.tenantScope()
        );
    }

    private static final class FakeScheduleEvaluator implements ScheduleEvaluator {
        private EvaluationResult evaluationResult = new EvaluationResult(List.of(), null);

        @Override
        public EvaluationResult evaluate(JobDefinition definition, ScheduleCursor cursor, Instant now, int maxBatch) {
            return evaluationResult;
        }
    }

    private static final class FakeRepository extends ExecutionRepositoryStub {
        private final List<InsertCall> inserts = new ArrayList<>();

        @Override
        public boolean enqueueScheduledExecution(ScheduledExecutionInsert request, Instant createdAt) {
            inserts.add(new InsertCall(request, createdAt));
            return true;
        }
    }

    private record InsertCall(org.jobgovernance.storage.spi.ExecutionRepository.ScheduledExecutionInsert request, Instant createdAt) {
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

    private static final class FakeScheduleCursorRepository implements ScheduleCursorRepository {
        private ScheduleCursor cursor;
        private ScheduleCursor lastUpdatedCursor;

        @Override
        public Optional<ScheduleCursor> findByJobKey(String jobKey) {
            return Optional.ofNullable(cursor);
        }

        @Override
        public void updateCursor(ScheduleCursor cursor, Instant updatedAt) {
            this.lastUpdatedCursor = cursor;
            this.cursor = new ScheduleCursor(
                    cursor.jobKey(),
                    cursor.lastEvaluatedAt(),
                    cursor.nextMaterializeAt(),
                    cursor.cursorVersion() + 1
            );
        }
    }

    private static final class FakeJobDefinitionRepository implements JobDefinitionRepository {
        private final List<JobDefinition> upserts = new ArrayList<>();

        @Override
        public void upsert(JobDefinition definition, Instant now) {
            upserts.add(definition);
        }

        @Override
        public Optional<JobDefinition> findByJobKey(String jobKey) {
            return upserts.stream().filter(definition -> definition.jobKey().equals(jobKey)).findFirst();
        }

        @Override
        public List<JobDefinition> findEnabled(int limit) {
            return upserts.stream().filter(definition -> definition.state() == JobDefinitionState.ENABLED).toList();
        }

        @Override
        public boolean updateState(String jobKey, JobDefinitionState targetState, String actor, Instant changedAt) {
            return false;
        }
    }
}
