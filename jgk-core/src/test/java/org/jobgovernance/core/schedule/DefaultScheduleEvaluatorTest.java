package org.jobgovernance.core.schedule;

import org.jobgovernance.core.api.ExecutionContext;
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

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultScheduleEvaluatorTest {

    private final DefaultScheduleEvaluator evaluator = new DefaultScheduleEvaluator();

    @Test
    void shouldEvaluateOneTimeScheduleOnlyOnce() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        JobDefinition definition = definition(
                "job-one-time",
                new JobSchedule.OneTimeSchedule(now.minusSeconds(5), ZoneId.of("UTC"), null, null),
                MisfirePolicy.CATCH_UP_ALL_MISSED_WINDOWS
        );

        List<ScheduleEvaluator.DueExecutionCandidate> first = evaluator.evaluateDue(definition, null, now, 10);
        List<ScheduleEvaluator.DueExecutionCandidate> second = evaluator.evaluateDue(
                definition,
                new ScheduleEvaluator.ScheduleCursor(definition.jobKey(), now, null),
                now,
                10
        );

        assertEquals(1, first.size());
        assertEquals(now.minusSeconds(5), first.getFirst().scheduledAt());
        assertTrue(second.isEmpty());
    }

    @Test
    void shouldCatchUpAllMissedFixedRateWindows() {
        Instant now = Instant.parse("2026-01-01T00:03:00Z");
        JobDefinition definition = definition(
                "job-fixed-rate",
                new JobSchedule.FixedRateSchedule(Duration.ofMinutes(1), Duration.ZERO, ZoneId.of("UTC"), now.minusSeconds(180), null),
                MisfirePolicy.CATCH_UP_ALL_MISSED_WINDOWS
        );
        ScheduleEvaluator.ScheduleCursor cursor = new ScheduleEvaluator.ScheduleCursor(
                definition.jobKey(),
                now.minusSeconds(180),
                now.minusSeconds(180)
        );

        List<ScheduleEvaluator.DueExecutionCandidate> due = evaluator.evaluateDue(definition, cursor, now, 10);

        assertEquals(4, due.size());
        assertEquals(now.minusSeconds(180), due.get(0).scheduledAt());
        assertEquals(now.minusSeconds(120), due.get(1).scheduledAt());
        assertEquals(now.minusSeconds(60), due.get(2).scheduledAt());
        assertEquals(now, due.get(3).scheduledAt());
    }

    @Test
    void shouldCatchUpOnlyLatestMissedWindowForFixedRate() {
        Instant now = Instant.parse("2026-01-01T00:03:30Z");
        Instant effectiveFrom = Instant.parse("2026-01-01T00:00:00Z");
        JobDefinition definition = definition(
                "job-latest-only",
                new JobSchedule.FixedRateSchedule(Duration.ofMinutes(1), Duration.ZERO, ZoneId.of("UTC"), effectiveFrom, null),
                MisfirePolicy.CATCH_UP_LATEST_ONLY
        );
        ScheduleEvaluator.ScheduleCursor cursor = new ScheduleEvaluator.ScheduleCursor(
                definition.jobKey(),
                effectiveFrom,
                effectiveFrom
        );

        List<ScheduleEvaluator.DueExecutionCandidate> due = evaluator.evaluateDue(definition, cursor, now, 10);

        assertEquals(1, due.size());
        assertEquals(Instant.parse("2026-01-01T00:03:00Z"), due.getFirst().scheduledAt());
    }

    @Test
    void shouldRunOnceImmediatelyWhenMisfirePolicyRequiresIt() {
        Instant now = Instant.parse("2026-01-01T00:03:30Z");
        Instant effectiveFrom = Instant.parse("2026-01-01T00:00:00Z");
        JobDefinition definition = definition(
                "job-run-now",
                new JobSchedule.FixedRateSchedule(Duration.ofMinutes(1), Duration.ZERO, ZoneId.of("UTC"), effectiveFrom, null),
                MisfirePolicy.RUN_ONCE_IMMEDIATELY
        );
        ScheduleEvaluator.ScheduleCursor cursor = new ScheduleEvaluator.ScheduleCursor(
                definition.jobKey(),
                effectiveFrom,
                effectiveFrom
        );

        List<ScheduleEvaluator.DueExecutionCandidate> due = evaluator.evaluateDue(definition, cursor, now, 10);

        assertEquals(1, due.size());
        assertEquals(now, due.getFirst().scheduledAt());
        assertEquals("CATCH_UP", due.getFirst().triggerType());
    }

    @Test
    void shouldEvaluateCronSchedules() {
        Instant now = Instant.parse("2026-01-01T00:16:00Z");
        JobDefinition definition = definition(
                "job-cron",
                new JobSchedule.CronSchedule("*/5 * * * *", ZoneId.of("UTC"), null, null),
                MisfirePolicy.CATCH_UP_ALL_MISSED_WINDOWS
        );
        ScheduleEvaluator.ScheduleCursor cursor = new ScheduleEvaluator.ScheduleCursor(
                definition.jobKey(),
                now.minusSeconds(960),
                Instant.parse("2026-01-01T00:00:00Z")
        );

        List<ScheduleEvaluator.DueExecutionCandidate> due = evaluator.evaluateDue(definition, cursor, now, 10);

        assertEquals(4, due.size());
        assertEquals(Instant.parse("2026-01-01T00:00:00Z"), due.get(0).scheduledAt());
        assertEquals(Instant.parse("2026-01-01T00:05:00Z"), due.get(1).scheduledAt());
        assertEquals(Instant.parse("2026-01-01T00:10:00Z"), due.get(2).scheduledAt());
        assertEquals(Instant.parse("2026-01-01T00:15:00Z"), due.get(3).scheduledAt());
    }

    @Test
    void shouldRejectUnsupportedCronFormats() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        JobDefinition definition = definition(
                "job-invalid-cron",
                new JobSchedule.CronSchedule("* * *", ZoneId.of("UTC"), null, null),
                MisfirePolicy.CATCH_UP_ALL_MISSED_WINDOWS
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> evaluator.evaluateDue(definition, null, now, 10)
        );
    }

    private static JobDefinition definition(String jobKey, JobSchedule schedule, MisfirePolicy misfirePolicy) {
        return new JobDefinition(
                jobKey,
                1,
                jobKey,
                "test",
                "team",
                Set.of("test"),
                ExecutionMode.BLOCKING,
                schedule,
                new JobPolicy(
                        RetryStrategies.neverRetry(),
                        new TimeoutPolicy.DefaultTimeoutPolicy(
                                Duration.ofMinutes(1),
                                Duration.ofMinutes(1),
                                Duration.ofMinutes(1),
                                Duration.ofSeconds(10),
                                Duration.ofSeconds(30)
                        ),
                        misfirePolicy,
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
