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

public final class CleanupLoop implements ExecutionEngine.RunnableLoop {

    private static final Logger log = LoggerFactory.getLogger(CleanupLoop.class);

    private final ExecutionRepository executionRepository;
    private final Duration interval;
    private final Duration retention;
    private final int batchSize;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public CleanupLoop(
            ExecutionRepository executionRepository,
            Duration interval,
            Duration retention,
            int batchSize
    ) {
        this(executionRepository, interval, retention, batchSize, Clock.systemUTC());
    }

    CleanupLoop(
            ExecutionRepository executionRepository,
            Duration interval,
            Duration retention,
            int batchSize,
            Clock clock
    ) {
        this.executionRepository = Objects.requireNonNull(executionRepository, "executionRepository is required");
        if (interval == null || interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive");
        }
        if (retention == null || retention.isZero() || retention.isNegative()) {
            throw new IllegalArgumentException("retention must be positive");
        }
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }
        this.interval = interval;
        this.retention = retention;
        this.batchSize = batchSize;
        this.clock = Objects.requireNonNull(clock, "clock is required");
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().name("jgk-cleanup-loop-", 0).factory()
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
        log.info("JGK cleanup loop started");
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        scheduler.shutdownNow();
        log.info("JGK cleanup loop stopped");
    }

    int cleanupOnce(Instant now) {
        Instant cutoff = now.minus(retention);
        return executionRepository.cleanupFinishedExecutions(cutoff, batchSize, now);
    }

    private void tickSafely() {
        if (!running.get()) {
            return;
        }
        try {
            int deleted = cleanupOnce(clock.instant());
            if (deleted > 0) {
                log.info("Cleanup loop removed {} finished execution row(s)", deleted);
            }
        } catch (RuntimeException exception) {
            log.warn("Cleanup loop failed", exception);
        }
    }
}
