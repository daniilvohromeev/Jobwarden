package org.jobgovernance.executor;

import org.jobgovernance.core.api.CancellationToken;
import org.jobgovernance.core.api.ExecutionContext;
import org.jobgovernance.core.api.JobRegistry;
import org.jobgovernance.core.model.JobExecution;
import org.jobgovernance.storage.spi.ExecutionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class HandlerRunnerLoop implements ExecutionEngine.RunnableLoop {

    private static final Logger log = LoggerFactory.getLogger(HandlerRunnerLoop.class);

    private final BlockingQueue<JobExecution> claimedQueue;
    private final JobRegistry jobRegistry;
    private final ExecutionRepository executionRepository;
    private final String workerId;
    private final ExecutorService workerPool;
    private final Clock clock;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile Thread dispatcherThread;

    public HandlerRunnerLoop(
            BlockingQueue<JobExecution> claimedQueue,
            JobRegistry jobRegistry,
            ExecutionRepository executionRepository,
            String workerId,
            int maxParallelHandlers
    ) {
        this(claimedQueue, jobRegistry, executionRepository, workerId, maxParallelHandlers, Clock.systemUTC());
    }

    HandlerRunnerLoop(
            BlockingQueue<JobExecution> claimedQueue,
            JobRegistry jobRegistry,
            ExecutionRepository executionRepository,
            String workerId,
            int maxParallelHandlers,
            Clock clock
    ) {
        this.claimedQueue = Objects.requireNonNull(claimedQueue, "claimedQueue is required");
        this.jobRegistry = Objects.requireNonNull(jobRegistry, "jobRegistry is required");
        this.executionRepository = Objects.requireNonNull(executionRepository, "executionRepository is required");
        if (workerId == null || workerId.isBlank()) {
            throw new IllegalArgumentException("workerId is required");
        }
        if (maxParallelHandlers < 1) {
            throw new IllegalArgumentException("maxParallelHandlers must be >= 1");
        }
        this.workerId = workerId;
        this.clock = Objects.requireNonNull(clock, "clock is required");
        this.workerPool = Executors.newFixedThreadPool(maxParallelHandlers, Thread.ofPlatform().name("jgk-runner-worker-", 0).factory());
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        dispatcherThread = Thread.ofPlatform()
                .name("jgk-runner-dispatcher")
                .start(this::dispatchLoop);
        log.info("JGK runner loop started for workerId={}", workerId);
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        Thread dispatcher = dispatcherThread;
        if (dispatcher != null) {
            dispatcher.interrupt();
            try {
                dispatcher.join(5_000);
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
            }
        }
        workerPool.shutdown();
        try {
            if (!workerPool.awaitTermination(10, TimeUnit.SECONDS)) {
                workerPool.shutdownNow();
            }
        } catch (InterruptedException interruptedException) {
            workerPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("JGK runner loop stopped for workerId={}", workerId);
    }

    private void dispatchLoop() {
        while (running.get() || !claimedQueue.isEmpty()) {
            try {
                JobExecution claimed = claimedQueue.poll(250, TimeUnit.MILLISECONDS);
                if (claimed == null) {
                    continue;
                }
                workerPool.submit(() -> executeClaimed(claimed));
            } catch (InterruptedException interruptedException) {
                if (!running.get()) {
                    Thread.currentThread().interrupt();
                    return;
                }
            } catch (RuntimeException exception) {
                log.warn("Runner dispatcher failure for workerId={}", workerId, exception);
            }
        }
    }

    @SuppressWarnings("unchecked")
    void executeClaimed(JobExecution claimed) {
        Instant startedAt = clock.instant();
        boolean runningMarked = executionRepository.markRunning(claimed.executionId(), workerId, claimed.leaseToken(), startedAt);
        if (!runningMarked) {
            return;
        }

        JobRegistry.JobRegistration<?, ?> registration = jobRegistry.findByJobKey(claimed.jobKey()).orElse(null);
        if (registration == null) {
            executionRepository.markFailedFinal(
                    claimed.executionId(),
                    workerId,
                    claimed.leaseToken(),
                    IllegalStateException.class.getName(),
                    "No registered handler for jobKey=" + claimed.jobKey(),
                    clock.instant()
            );
            return;
        }

        ExecutionContext<Object> context = new ExecutionContext<>(
                claimed.executionId(),
                claimed.jobKey(),
                claimed.attempt(),
                claimed.triggerType(),
                claimed.scheduledAt(),
                workerId,
                claimed.leaseToken(),
                claimed.correlationId(),
                claimed.traceId(),
                claimed.causationId(),
                claimed.parentExecutionId(),
                claimed.tenantId(),
                null,
                Map.of()
        );

        try {
            JobRegistry.JobRegistration<Object, Object> typed = (JobRegistry.JobRegistration<Object, Object>) registration;
            Object result;
            if (typed.handlerType() == JobRegistry.HandlerType.SYNC) {
                result = typed.syncHandler().handle(context, CancellationToken.none());
            } else {
                result = typed.asyncHandler().handleAsync(context, CancellationToken.none()).toCompletableFuture().join();
            }
            executionRepository.markSucceeded(
                    claimed.executionId(),
                    workerId,
                    claimed.leaseToken(),
                    summarizeResult(result),
                    clock.instant()
            );
        } catch (Exception exception) {
            executionRepository.markFailedFinal(
                    claimed.executionId(),
                    workerId,
                    claimed.leaseToken(),
                    exception.getClass().getName(),
                    summarizeError(exception),
                    clock.instant()
            );
        }
    }

    private static String summarizeResult(Object result) {
        if (result == null) {
            return "null";
        }
        return truncate(result.toString(), 1_024);
    }

    private static String summarizeError(Throwable throwable) {
        String message = throwable.getMessage();
        if (message == null || message.isBlank()) {
            return throwable.getClass().getSimpleName();
        }
        return truncate(message, 2_048);
    }

    private static String truncate(String value, int maxLength) {
        if (value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength);
    }
}
