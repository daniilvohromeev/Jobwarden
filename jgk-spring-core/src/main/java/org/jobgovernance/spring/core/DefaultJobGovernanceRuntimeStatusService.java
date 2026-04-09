package org.jobgovernance.spring.core;

import org.jobgovernance.core.api.JobRegistry;
import org.jobgovernance.executor.ActiveExecutionTracker;
import org.jobgovernance.executor.ExecutionEngine;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

final class DefaultJobGovernanceRuntimeStatusService implements JobGovernanceRuntimeStatusService {

    private final ExecutionEngine executionEngine;
    private final ActiveExecutionTracker activeExecutionTracker;
    private final JobRegistry jobRegistry;
    private final String workerId;
    private final Clock clock;

    DefaultJobGovernanceRuntimeStatusService(
            ExecutionEngine executionEngine,
            ActiveExecutionTracker activeExecutionTracker,
            JobRegistry jobRegistry,
            String workerId,
            Clock clock
    ) {
        this.executionEngine = Objects.requireNonNull(executionEngine, "executionEngine is required");
        this.activeExecutionTracker = Objects.requireNonNull(activeExecutionTracker, "activeExecutionTracker is required");
        this.jobRegistry = Objects.requireNonNull(jobRegistry, "jobRegistry is required");
        this.workerId = workerId == null || workerId.isBlank() ? "unknown-worker" : workerId;
        this.clock = Objects.requireNonNull(clock, "clock is required");
    }

    @Override
    public RuntimeStatus health() {
        return status(executionEngine.isRunning() ? "UP" : "DOWN");
    }

    @Override
    public RuntimeStatus readiness() {
        return status(executionEngine.isRunning() ? "READY" : "NOT_READY");
    }

    private RuntimeStatus status(String state) {
        Instant now = clock.instant();
        return new RuntimeStatus(
                state,
                workerId,
                executionEngine.isRunning(),
                executionEngine.loopCount(),
                activeExecutionTracker.snapshot().size(),
                jobRegistry.all().size(),
                now
        );
    }
}
