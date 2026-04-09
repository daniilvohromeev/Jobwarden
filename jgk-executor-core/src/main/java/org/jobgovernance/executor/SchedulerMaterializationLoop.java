package org.jobgovernance.executor;

import org.jobgovernance.core.api.JobRegistry;
import org.jobgovernance.core.api.ScheduleEvaluator;
import org.jobgovernance.core.model.JobDefinition;
import org.jobgovernance.core.model.JobDefinitionState;
import org.jobgovernance.core.model.JobSchedule;
import org.jobgovernance.storage.spi.ExecutionRepository;
import org.jobgovernance.storage.spi.JobDefinitionRepository;
import org.jobgovernance.storage.spi.ScheduleCursorRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class SchedulerMaterializationLoop implements ExecutionEngine.RunnableLoop {

    private static final Logger log = LoggerFactory.getLogger(SchedulerMaterializationLoop.class);
    private static final int DEFAULT_MAX_ATTEMPTS = 3;

    private final JobRegistry jobRegistry;
    private final ScheduleEvaluator scheduleEvaluator;
    private final ExecutionRepository executionRepository;
    private final ScheduleCursorRepository scheduleCursorRepository;
    private final JobDefinitionRepository jobDefinitionRepository;
    private final int perJobBatch;
    private final Duration interval;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ConcurrentMap<String, ScheduleEvaluator.ScheduleCursor> inMemoryCursors = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> cursorVersions = new ConcurrentHashMap<>();

    public SchedulerMaterializationLoop(
            JobRegistry jobRegistry,
            ScheduleEvaluator scheduleEvaluator,
            ExecutionRepository executionRepository,
            int perJobBatch,
            Duration interval
    ) {
        this(
                jobRegistry,
                scheduleEvaluator,
                executionRepository,
                null,
                null,
                perJobBatch,
                interval,
                Clock.systemUTC()
        );
    }

    public SchedulerMaterializationLoop(
            JobRegistry jobRegistry,
            ScheduleEvaluator scheduleEvaluator,
            ExecutionRepository executionRepository,
            ScheduleCursorRepository scheduleCursorRepository,
            JobDefinitionRepository jobDefinitionRepository,
            int perJobBatch,
            Duration interval
    ) {
        this(
                jobRegistry,
                scheduleEvaluator,
                executionRepository,
                scheduleCursorRepository,
                jobDefinitionRepository,
                perJobBatch,
                interval,
                Clock.systemUTC()
        );
    }

    SchedulerMaterializationLoop(
            JobRegistry jobRegistry,
            ScheduleEvaluator scheduleEvaluator,
            ExecutionRepository executionRepository,
            ScheduleCursorRepository scheduleCursorRepository,
            JobDefinitionRepository jobDefinitionRepository,
            int perJobBatch,
            Duration interval,
            Clock clock
    ) {
        this.jobRegistry = Objects.requireNonNull(jobRegistry, "jobRegistry is required");
        this.scheduleEvaluator = Objects.requireNonNull(scheduleEvaluator, "scheduleEvaluator is required");
        this.executionRepository = Objects.requireNonNull(executionRepository, "executionRepository is required");
        this.scheduleCursorRepository = scheduleCursorRepository;
        this.jobDefinitionRepository = jobDefinitionRepository;
        if (perJobBatch < 1) {
            throw new IllegalArgumentException("perJobBatch must be >= 1");
        }
        if (interval == null || interval.isNegative() || interval.isZero()) {
            throw new IllegalArgumentException("interval must be positive");
        }
        this.perJobBatch = perJobBatch;
        this.interval = interval;
        this.clock = Objects.requireNonNull(clock, "clock is required");
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().name("jgk-scheduler-loop-", 0).factory()
        );
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        scheduler.scheduleWithFixedDelay(
                this::tickSafely,
                0L,
                interval.toMillis(),
                TimeUnit.MILLISECONDS
        );
        log.info("JGK scheduler materialization loop started");
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        scheduler.shutdownNow();
        log.info("JGK scheduler materialization loop stopped");
    }

    int materializeOnce(Instant now) {
        int inserted = 0;
        for (JobRegistry.JobRegistration<?, ?> registration : jobRegistry.all()) {
            JobDefinition definition = resolveSchedulingDefinition(registration.definition(), now);
            if (definition.state() != JobDefinitionState.ENABLED || definition.schedule().kind() == JobSchedule.Kind.DISABLED) {
                continue;
            }
            String jobKey = definition.jobKey();

            ScheduleEvaluator.ScheduleCursor cursor = loadCursor(jobKey);
            ScheduleEvaluator.EvaluationResult evaluation = scheduleEvaluator.evaluate(
                    definition,
                    cursor,
                    now,
                    perJobBatch
            );
            int maxAttempts = definition.policy().retryStrategy()
                    .maxAttemptsHint()
                    .orElse(DEFAULT_MAX_ATTEMPTS);

            for (ScheduleEvaluator.DueExecutionCandidate candidate : evaluation.dueCandidates()) {
                boolean enqueued = executionRepository.enqueueScheduledExecution(
                        new ExecutionRepository.ScheduledExecutionInsert(
                                jobKey,
                                definition.tenantScope(),
                                candidate.triggerType(),
                                candidate.scheduledAt(),
                                candidate.scheduledAt(),
                                maxAttempts,
                                null,
                                candidate.dedupeKey(),
                                null,
                                null,
                                null,
                                null,
                                null,
                                null
                        ),
                        now
                );
                if (enqueued) {
                    inserted++;
                }
            }

            storeCursor(jobKey, now, evaluation.nextFireAt());
        }
        return inserted;
    }

    Map<String, ScheduleEvaluator.ScheduleCursor> snapshotCursors() {
        return Map.copyOf(inMemoryCursors);
    }

    private void tickSafely() {
        if (!running.get()) {
            return;
        }
        try {
            int inserted = materializeOnce(clock.instant());
            if (inserted > 0) {
                log.info("Scheduler loop materialized {} execution(s)", inserted);
            }
        } catch (RuntimeException exception) {
            log.warn("Scheduler loop failed", exception);
        }
    }

    private ScheduleEvaluator.ScheduleCursor loadCursor(String jobKey) {
        if (scheduleCursorRepository != null) {
            return scheduleCursorRepository.findByJobKey(jobKey)
                    .map(cursor -> {
                        ScheduleEvaluator.ScheduleCursor mapped = new ScheduleEvaluator.ScheduleCursor(
                                cursor.jobKey(),
                                cursor.lastEvaluatedAt(),
                                cursor.nextMaterializeAt()
                        );
                        inMemoryCursors.put(jobKey, mapped);
                        cursorVersions.put(jobKey, cursor.cursorVersion());
                        return mapped;
                    })
                    .orElseGet(() -> {
                        cursorVersions.putIfAbsent(jobKey, 0L);
                        return inMemoryCursors.get(jobKey);
                    });
        }
        return inMemoryCursors.get(jobKey);
    }

    private void storeCursor(
            String jobKey,
            Instant evaluatedAt,
            Instant nextFireAt
    ) {
        ScheduleEvaluator.ScheduleCursor current = new ScheduleEvaluator.ScheduleCursor(jobKey, evaluatedAt, nextFireAt);
        inMemoryCursors.put(jobKey, current);
        if (scheduleCursorRepository == null) {
            return;
        }
        long cursorVersion = cursorVersions.getOrDefault(jobKey, 0L);
        try {
            scheduleCursorRepository.updateCursor(
                    new ScheduleCursorRepository.ScheduleCursor(jobKey, evaluatedAt, nextFireAt, cursorVersion),
                    evaluatedAt
            );
            cursorVersions.put(jobKey, cursorVersion + 1);
        } catch (RuntimeException exception) {
            log.debug("Failed to persist cursor for jobKey={}, continuing with in-memory cursor", jobKey, exception);
        }
    }

    private JobDefinition resolveSchedulingDefinition(JobDefinition registryDefinition, Instant now) {
        if (jobDefinitionRepository == null) {
            return registryDefinition;
        }
        return jobDefinitionRepository.findByJobKey(registryDefinition.jobKey())
                .map(persistedDefinition -> chooseDefinition(registryDefinition, persistedDefinition, now))
                .orElseGet(() -> {
                    jobDefinitionRepository.upsert(registryDefinition, now);
                    return registryDefinition;
                });
    }

    private JobDefinition chooseDefinition(
            JobDefinition registryDefinition,
            JobDefinition persistedDefinition,
            Instant now
    ) {
        if (registryDefinition.version() > persistedDefinition.version()) {
            jobDefinitionRepository.upsert(registryDefinition, now);
            return registryDefinition;
        }
        return persistedDefinition;
    }
}
