package org.jobgovernance.executor;

import org.jobgovernance.core.api.ClaimService;
import org.jobgovernance.core.model.JobExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public final class PollingClaimLoop implements ExecutionEngine.RunnableLoop {

    private static final Logger log = LoggerFactory.getLogger(PollingClaimLoop.class);

    private final ClaimService claimService;
    private final String workerId;
    private final int batchSize;
    private final Duration minBackoff;
    private final Duration maxBackoff;
    private final Consumer<JobExecution> claimedExecutionSink;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;
    private final AtomicReference<ScheduledFuture<?>> nextRunRef = new AtomicReference<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile Duration currentBackoff;

    public PollingClaimLoop(
            ClaimService claimService,
            String workerId,
            int batchSize,
            Duration minBackoff,
            Duration maxBackoff,
            Consumer<JobExecution> claimedExecutionSink
    ) {
        this(claimService, workerId, batchSize, minBackoff, maxBackoff, claimedExecutionSink, Clock.systemUTC());
    }

    PollingClaimLoop(
            ClaimService claimService,
            String workerId,
            int batchSize,
            Duration minBackoff,
            Duration maxBackoff,
            Consumer<JobExecution> claimedExecutionSink,
            Clock clock
    ) {
        this.claimService = Objects.requireNonNull(claimService, "claimService is required");
        this.workerId = requireWorkerId(workerId);
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }
        this.batchSize = batchSize;
        this.minBackoff = requirePositive(minBackoff, "minBackoff");
        this.maxBackoff = requirePositive(maxBackoff, "maxBackoff");
        if (this.maxBackoff.compareTo(this.minBackoff) < 0) {
            throw new IllegalArgumentException("maxBackoff must be >= minBackoff");
        }
        this.claimedExecutionSink = Objects.requireNonNull(claimedExecutionSink, "claimedExecutionSink is required");
        this.clock = Objects.requireNonNull(clock, "clock is required");
        ThreadFactory threadFactory = Thread.ofPlatform().name("jgk-claim-loop-", 0).factory();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(threadFactory);
        this.currentBackoff = this.minBackoff;
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        currentBackoff = minBackoff;
        scheduleNext(Duration.ZERO);
        log.info("JGK claim loop started for workerId={}", workerId);
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        ScheduledFuture<?> nextRun = nextRunRef.getAndSet(null);
        if (nextRun != null) {
            nextRun.cancel(false);
        }
        scheduler.shutdownNow();
        log.info("JGK claim loop stopped for workerId={}", workerId);
    }

    private void pollAndReschedule() {
        if (!running.get()) {
            return;
        }
        try {
            List<JobExecution> claimed = claimService.claimDueExecutions(workerId, batchSize, clock.instant());
            if (claimed.isEmpty()) {
                currentBackoff = nextBackoff(currentBackoff);
            } else {
                currentBackoff = minBackoff;
                claimed.forEach(claimedExecutionSink);
            }
        } catch (RuntimeException exception) {
            currentBackoff = nextBackoff(currentBackoff);
            log.warn("Claim loop poll failed for workerId={}", workerId, exception);
        }
        scheduleNext(currentBackoff);
    }

    private Duration nextBackoff(Duration current) {
        Duration doubled = current.multipliedBy(2);
        return doubled.compareTo(maxBackoff) > 0 ? maxBackoff : doubled;
    }

    private void scheduleNext(Duration delay) {
        ScheduledFuture<?> future = scheduler.schedule(this::pollAndReschedule, delay.toMillis(), TimeUnit.MILLISECONDS);
        ScheduledFuture<?> previous = nextRunRef.getAndSet(future);
        if (previous != null && !previous.isDone()) {
            previous.cancel(false);
        }
    }

    private static String requireWorkerId(String workerId) {
        if (workerId == null || workerId.isBlank()) {
            throw new IllegalArgumentException("workerId is required");
        }
        return workerId;
    }

    private static Duration requirePositive(Duration value, String name) {
        if (value == null || value.isNegative() || value.isZero()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }
}
