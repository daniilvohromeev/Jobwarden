package org.jobgovernance.executor;

import org.jobgovernance.core.api.JobRegistry;
import org.jobgovernance.core.api.ScheduleEvaluator;
import org.jobgovernance.core.model.JobDefinition;
import org.jobgovernance.storage.spi.ExecutionRepository;
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
    private final int perJobBatch;
    private final Duration interval;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ConcurrentMap<String, ScheduleEvaluator.ScheduleCursor> cursors = new ConcurrentHashMap<>();

    public SchedulerMaterializationLoop(
            JobRegistry jobRegistry,
            ScheduleEvaluator scheduleEvaluator,
            ExecutionRepository executionRepository,
            int perJobBatch,
            Duration interval
    ) {
        this(jobRegistry, scheduleEvaluator, executionRepository, perJobBatch, interval, Clock.systemUTC());
    }

    SchedulerMaterializationLoop(
            JobRegistry jobRegistry,
            ScheduleEvaluator scheduleEvaluator,
            ExecutionRepository executionRepository,
            int perJobBatch,
            Duration interval,
            Clock clock
    ) {
        this.jobRegistry = Objects.requireNonNull(jobRegistry, "jobRegistry is required");
        this.scheduleEvaluator = Objects.requireNonNull(scheduleEvaluator, "scheduleEvaluator is required");
        this.executionRepository = Objects.requireNonNull(executionRepository, "executionRepository is required");
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
            JobDefinition definition = registration.definition();
            String jobKey = definition.jobKey();
            ScheduleEvaluator.ScheduleCursor cursor = cursors.get(jobKey);
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

            cursors.put(jobKey, new ScheduleEvaluator.ScheduleCursor(jobKey, now, evaluation.nextFireAt()));
        }
        return inserted;
    }

    Map<String, ScheduleEvaluator.ScheduleCursor> snapshotCursors() {
        return Map.copyOf(cursors);
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
}
