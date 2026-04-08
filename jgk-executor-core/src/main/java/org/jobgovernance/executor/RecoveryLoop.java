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

public final class RecoveryLoop implements ExecutionEngine.RunnableLoop {

    private static final Logger log = LoggerFactory.getLogger(RecoveryLoop.class);

    private final ExecutionRepository executionRepository;
    private final String recoveryWorkerId;
    private final Duration interval;
    private final String deadReason;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public RecoveryLoop(
            ExecutionRepository executionRepository,
            String recoveryWorkerId,
            Duration interval,
            String deadReason
    ) {
        this(executionRepository, recoveryWorkerId, interval, deadReason, Clock.systemUTC());
    }

    RecoveryLoop(
            ExecutionRepository executionRepository,
            String recoveryWorkerId,
            Duration interval,
            String deadReason,
            Clock clock
    ) {
        this.executionRepository = Objects.requireNonNull(executionRepository, "executionRepository is required");
        if (recoveryWorkerId == null || recoveryWorkerId.isBlank()) {
            throw new IllegalArgumentException("recoveryWorkerId is required");
        }
        if (interval == null || interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive");
        }
        this.recoveryWorkerId = recoveryWorkerId;
        this.interval = interval;
        this.deadReason = deadReason == null || deadReason.isBlank() ? "recovery deadline exceeded" : deadReason;
        this.clock = Objects.requireNonNull(clock, "clock is required");
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().name("jgk-recovery-loop-", 0).factory()
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
        log.info("JGK recovery loop started for workerId={}", recoveryWorkerId);
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        scheduler.shutdownNow();
        log.info("JGK recovery loop stopped for workerId={}", recoveryWorkerId);
    }

    RecoveryOutcome recoverOnce(Instant now) {
        int recovered = executionRepository.recoverStaleClaims(now, recoveryWorkerId, now);
        int dead = executionRepository.markDeadExecutions(now, deadReason, now);
        return new RecoveryOutcome(recovered, dead);
    }

    private void tickSafely() {
        if (!running.get()) {
            return;
        }
        try {
            RecoveryOutcome outcome = recoverOnce(clock.instant());
            if (outcome.recoveredCount() > 0 || outcome.markedDeadCount() > 0) {
                log.info(
                        "Recovery loop processed stale claims recovered={} dead={}",
                        outcome.recoveredCount(),
                        outcome.markedDeadCount()
                );
            }
        } catch (RuntimeException exception) {
            log.warn("Recovery loop failed for workerId={}", recoveryWorkerId, exception);
        }
    }

    record RecoveryOutcome(int recoveredCount, int markedDeadCount) {
    }
}
