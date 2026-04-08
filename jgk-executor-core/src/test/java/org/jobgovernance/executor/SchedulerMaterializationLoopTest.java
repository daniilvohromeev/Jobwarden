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
                10,
                Duration.ofSeconds(5),
                Clock.fixed(now, ZoneOffset.UTC)
        );

        loop.materializeOnce(now);

        assertEquals(1, repository.inserts.size());
        assertEquals(3, repository.inserts.getFirst().request().maxAttempts());
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
}
