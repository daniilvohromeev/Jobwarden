package org.jobgovernance.spring.core;

import java.time.Instant;

public interface JobGovernanceRuntimeStatusService {

    RuntimeStatus health();

    RuntimeStatus readiness();

    record RuntimeStatus(
            String state,
            String workerId,
            boolean engineRunning,
            int loopCount,
            int activeExecutions,
            int registeredJobs,
            Instant observedAt
    ) {
    }
}
