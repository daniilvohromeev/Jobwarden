package org.jobgovernance.executor;

import org.jobgovernance.storage.spi.ExecutionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class HeartbeatLoop implements ExecutionEngine.RunnableLoop {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatLoop.class);

    private final ExecutionRepository executionRepository;
    private final ActiveExecutionTracker activeExecutionTracker;
    private final Duration heartbeatInterval;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public HeartbeatLoop(
            ExecutionRepository executionRepository,
            ActiveExecutionTracker activeExecutionTracker,
            Duration heartbeatInterval
    ) {
        this(executionRepository, activeExecutionTracker, heartbeatInterval, Clock.systemUTC());
    }

    HeartbeatLoop(
            ExecutionRepository executionRepository,
            ActiveExecutionTracker activeExecutionTracker,
            Duration heartbeatInterval,
            Clock clock
    ) {
        this.executionRepository = Objects.requireNonNull(executionRepository, "executionRepository is required");
        this.activeExecutionTracker = Objects.requireNonNull(activeExecutionTracker, "activeExecutionTracker is required");
        if (heartbeatInterval == null || heartbeatInterval.isZero() || heartbeatInterval.isNegative()) {
            throw new IllegalArgumentException("heartbeatInterval must be positive");
        }
        this.heartbeatInterval = heartbeatInterval;
        this.clock = Objects.requireNonNull(clock, "clock is required");
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().name("jgk-heartbeat-loop-", 0).factory()
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
                heartbeatInterval.toMillis(),
                TimeUnit.MILLISECONDS
        );
        log.info("JGK heartbeat loop started");
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        scheduler.shutdownNow();
        log.info("JGK heartbeat loop stopped");
    }

    int renewLeasesOnce(Instant now) {
        Collection<ActiveExecutionTracker.ActiveExecution> active = activeExecutionTracker.snapshot();
        int renewed = 0;
        for (ActiveExecutionTracker.ActiveExecution execution : active) {
            try {
                boolean success = executionRepository.renewLease(
                        execution.executionId(),
                        execution.workerId(),
                        execution.leaseToken(),
                        now.plus(execution.leaseTtl()),
                        now
                );
                if (success) {
                    renewed++;
                } else {
                    activeExecutionTracker.remove(execution.executionId());
                    log.warn(
                            "Lease renewal rejected for executionId={} workerId={}, removing from active tracker",
                            execution.executionId(),
                            execution.workerId()
                    );
                }
            } catch (RuntimeException exception) {
                log.warn(
                        "Lease renewal failed for executionId={} workerId={}",
                        execution.executionId(),
                        execution.workerId(),
                        exception
                );
            }
        }
        return renewed;
    }

    private void tickSafely() {
        if (!running.get()) {
            return;
        }
        int renewed = renewLeasesOnce(clock.instant());
        if (renewed > 0) {
            log.debug("Renewed {} active leases", renewed);
        }
    }
}
