package org.jobgovernance.executor;

import org.jobgovernance.storage.spi.ExecutionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class RetryRequeueLoop implements ExecutionEngine.RunnableLoop {

    private static final Logger log = LoggerFactory.getLogger(RetryRequeueLoop.class);

    private final ExecutionRepository executionRepository;
    private final Duration interval;
    private final int batchSize;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public RetryRequeueLoop(
            ExecutionRepository executionRepository,
            Duration interval,
            int batchSize
    ) {
        this(executionRepository, interval, batchSize, Clock.systemUTC());
    }

    RetryRequeueLoop(
            ExecutionRepository executionRepository,
            Duration interval,
            int batchSize,
            Clock clock
    ) {
        this.executionRepository = Objects.requireNonNull(executionRepository, "executionRepository is required");
        if (interval == null || interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive");
        }
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }
        this.interval = interval;
        this.batchSize = batchSize;
        this.clock = Objects.requireNonNull(clock, "clock is required");
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().name("jgk-retry-loop-", 0).factory()
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
        log.info("JGK retry requeue loop started");
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        scheduler.shutdownNow();
        log.info("JGK retry requeue loop stopped");
    }

    int requeueDueRetriesOnce(Instant now) {
        return executionRepository.requeueRetryableExecutions(now, batchSize, now);
    }

    private void tickSafely() {
        if (!running.get()) {
            return;
        }
        try {
            int requeued = requeueDueRetriesOnce(clock.instant());
            if (requeued > 0) {
                log.info("Requeued {} retryable executions", requeued);
            }
        } catch (RuntimeException exception) {
            log.warn("Retry requeue loop failed", exception);
        }
    }
}
