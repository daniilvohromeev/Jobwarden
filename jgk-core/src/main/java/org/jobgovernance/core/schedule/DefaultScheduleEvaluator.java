package org.jobgovernance.core.schedule;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import org.jobgovernance.core.api.ScheduleEvaluator;
import org.jobgovernance.core.model.JobDefinition;
import org.jobgovernance.core.model.JobDefinitionState;
import org.jobgovernance.core.model.JobSchedule;
import org.jobgovernance.core.model.MisfirePolicy;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class DefaultScheduleEvaluator implements ScheduleEvaluator {

    private final CronParser unixCronParser;
    private final CronParser springCronParser;
    private final ConcurrentMap<CronCacheKey, ExecutionTime> executionTimeCache = new ConcurrentHashMap<>();

    public DefaultScheduleEvaluator() {
        this.unixCronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));
        this.springCronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.SPRING));
    }

    @Override
    public EvaluationResult evaluate(
            JobDefinition definition,
            ScheduleCursor cursor,
            Instant now,
            int maxBatch
    ) {
        Objects.requireNonNull(definition, "definition is required");
        Objects.requireNonNull(now, "now is required");
        if (maxBatch < 1) {
            throw new IllegalArgumentException("maxBatch must be >= 1");
        }
        if (definition.state() != JobDefinitionState.ENABLED) {
            return new EvaluationResult(List.of(), null);
        }

        ComputationResult computation = switch (definition.schedule()) {
            case JobSchedule.OneTimeSchedule oneTimeSchedule -> evaluateOneTime(definition, oneTimeSchedule, cursor, now);
            case JobSchedule.FixedDelaySchedule fixedDelaySchedule -> evaluateFixedDelay(definition, fixedDelaySchedule, cursor, now, maxBatch);
            case JobSchedule.FixedRateSchedule fixedRateSchedule -> evaluateFixedRate(definition, fixedRateSchedule, cursor, now, maxBatch);
            case JobSchedule.CronSchedule cronSchedule -> evaluateCron(definition, cronSchedule, cursor, now, maxBatch);
            case JobSchedule.DisabledSchedule ignored -> new ComputationResult(List.of(), null);
        };

        List<DueExecutionCandidate> due = applyMisfirePolicy(definition, computation.dueCandidates(), now, maxBatch);
        return new EvaluationResult(due, computation.nextFireAt());
    }

    private ComputationResult evaluateOneTime(
            JobDefinition definition,
            JobSchedule.OneTimeSchedule schedule,
            ScheduleCursor cursor,
            Instant now
    ) {
        Instant scheduledAt = schedule.scheduledAt();
        if (scheduledAt.isAfter(now)) {
            return new ComputationResult(List.of(), scheduledAt);
        }
        if (!schedule.isWithinEffectiveWindow(scheduledAt)) {
            return new ComputationResult(List.of(), null);
        }
        if (cursor != null && cursor.lastEvaluatedAt() != null && !cursor.lastEvaluatedAt().isBefore(scheduledAt)) {
            return new ComputationResult(List.of(), null);
        }
        return new ComputationResult(List.of(candidate(definition.jobKey(), scheduledAt, "DELAYED")), null);
    }

    private ComputationResult evaluateFixedDelay(
            JobDefinition definition,
            JobSchedule.FixedDelaySchedule schedule,
            ScheduleCursor cursor,
            Instant now,
            int maxBatch
    ) {
        Instant next = nextRecurringFire(schedule.initialDelay(), schedule.effectiveFrom(), cursor, now);
        return collectRecurringCandidates(
                definition.jobKey(),
                schedule,
                next,
                schedule.delay(),
                now,
                maxBatch,
                "FIXED_DELAY"
        );
    }

    private ComputationResult evaluateFixedRate(
            JobDefinition definition,
            JobSchedule.FixedRateSchedule schedule,
            ScheduleCursor cursor,
            Instant now,
            int maxBatch
    ) {
        Instant next = nextRecurringFire(schedule.initialDelay(), schedule.effectiveFrom(), cursor, now);
        return collectRecurringCandidates(
                definition.jobKey(),
                schedule,
                next,
                schedule.rate(),
                now,
                maxBatch,
                "FIXED_RATE"
        );
    }

    private ComputationResult collectRecurringCandidates(
            String jobKey,
            JobSchedule schedule,
            Instant next,
            java.time.Duration step,
            Instant now,
            int maxBatch,
            String triggerType
    ) {
        if (next == null) {
            return new ComputationResult(List.of(), null);
        }
        List<DueExecutionCandidate> due = new ArrayList<>(Math.min(8, maxBatch));
        int remainingSafetyIterations = maxBatch * 4;
        while (next != null && !next.isAfter(now) && due.size() < maxBatch && remainingSafetyIterations-- > 0) {
            if (schedule.isWithinEffectiveWindow(next)) {
                due.add(candidate(jobKey, next, triggerType));
            }
            next = next.plus(step);
        }
        return new ComputationResult(due, next);
    }

    private ComputationResult evaluateCron(
            JobDefinition definition,
            JobSchedule.CronSchedule schedule,
            ScheduleCursor cursor,
            Instant now,
            int maxBatch
    ) {
        ExecutionTime executionTime = executionTime(schedule.cronExpression());
        Instant next = initialCronFire(schedule, cursor, now, executionTime);
        if (next == null) {
            return new ComputationResult(List.of(), null);
        }
        List<DueExecutionCandidate> due = new ArrayList<>(Math.min(8, maxBatch));
        int remainingSafetyIterations = maxBatch * 8;
        while (next != null && !next.isAfter(now) && due.size() < maxBatch && remainingSafetyIterations-- > 0) {
            if (schedule.isWithinEffectiveWindow(next)) {
                due.add(candidate(definition.jobKey(), next, "CRON"));
            }
            next = nextCronFire(next, schedule.zoneId(), executionTime);
        }
        return new ComputationResult(due, next);
    }

    private static Instant nextRecurringFire(
            java.time.Duration initialDelay,
            Instant effectiveFrom,
            ScheduleCursor cursor,
            Instant now
    ) {
        if (cursor != null && cursor.nextFireAt() != null) {
            return cursor.nextFireAt();
        }
        Instant anchor = effectiveFrom == null ? now : effectiveFrom;
        return anchor.plus(initialDelay);
    }

    private static Instant initialCronFire(
            JobSchedule.CronSchedule schedule,
            ScheduleCursor cursor,
            Instant now,
            ExecutionTime executionTime
    ) {
        if (cursor != null && cursor.nextFireAt() != null) {
            return cursor.nextFireAt();
        }
        Instant anchor = schedule.effectiveFrom() == null ? now.minusSeconds(1) : schedule.effectiveFrom().minusSeconds(1);
        return nextCronFire(anchor, schedule.zoneId(), executionTime);
    }

    private static Instant nextCronFire(Instant from, ZoneId zoneId, ExecutionTime executionTime) {
        Optional<ZonedDateTime> next = executionTime.nextExecution(from.atZone(zoneId));
        return next.map(ZonedDateTime::toInstant).orElse(null);
    }

    private ExecutionTime executionTime(String cronExpression) {
        String expression = cronExpression == null ? "" : cronExpression.trim();
        int fields = expression.isEmpty() ? 0 : expression.split("\\s+").length;
        CronCacheKey key = new CronCacheKey(expression, fields);
        return executionTimeCache.computeIfAbsent(key, ignored -> buildExecutionTime(expression, fields));
    }

    private ExecutionTime buildExecutionTime(String expression, int fields) {
        Cron cron = switch (fields) {
            case 5 -> unixCronParser.parse(expression);
            case 6 -> springCronParser.parse(expression);
            default -> throw new IllegalArgumentException("Unsupported cron format; expected 5 or 6 fields");
        };
        cron.validate();
        return ExecutionTime.forCron(cron);
    }

    private List<DueExecutionCandidate> applyMisfirePolicy(
            JobDefinition definition,
            List<DueExecutionCandidate> dueCandidates,
            Instant now,
            int maxBatch
    ) {
        if (dueCandidates.isEmpty()) {
            return dueCandidates;
        }

        List<DueExecutionCandidate> missed = dueCandidates.stream()
                .filter(candidate -> candidate.scheduledAt().isBefore(now))
                .toList();
        List<DueExecutionCandidate> onTime = dueCandidates.stream()
                .filter(candidate -> !candidate.scheduledAt().isBefore(now))
                .toList();

        if (missed.isEmpty()) {
            return trim(dueCandidates, maxBatch);
        }

        MisfirePolicy misfirePolicy = definition.policy().misfirePolicy();
        return switch (misfirePolicy) {
            case CATCH_UP_ALL_MISSED_WINDOWS -> trim(dueCandidates, maxBatch);
            case CATCH_UP_LATEST_ONLY -> {
                List<DueExecutionCandidate> result = new ArrayList<>(Math.min(maxBatch, 2));
                result.add(missed.getLast());
                result.addAll(onTime);
                yield trim(result, maxBatch);
            }
            case RUN_ONCE_IMMEDIATELY -> {
                if (!onTime.isEmpty()) {
                    yield trim(onTime, maxBatch);
                }
                yield List.of(candidate(definition.jobKey(), now, "CATCH_UP"));
            }
            case IGNORE, MARK_MISFIRED_AND_SKIP -> trim(onTime, maxBatch);
        };
    }

    private static List<DueExecutionCandidate> trim(List<DueExecutionCandidate> candidates, int maxBatch) {
        if (candidates.size() <= maxBatch) {
            return List.copyOf(candidates);
        }
        return List.copyOf(candidates.subList(0, maxBatch));
    }

    private static DueExecutionCandidate candidate(String jobKey, Instant scheduledAt, String triggerType) {
        String dedupeKey = jobKey + "|" + scheduledAt.toEpochMilli() + "|" + triggerType;
        return new DueExecutionCandidate(jobKey, scheduledAt, triggerType, dedupeKey);
    }

    private record ComputationResult(List<DueExecutionCandidate> dueCandidates, Instant nextFireAt) {
    }

    private record CronCacheKey(String expression, int fields) {
    }
}
