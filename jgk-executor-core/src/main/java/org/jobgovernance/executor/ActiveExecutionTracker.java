package org.jobgovernance.executor;

import java.time.Duration;
import java.util.Collection;
import java.util.UUID;

public interface ActiveExecutionTracker {

    void add(ActiveExecution activeExecution);

    void remove(UUID executionId);

    Collection<ActiveExecution> snapshot();

    record ActiveExecution(
            UUID executionId,
            String workerId,
            String leaseToken,
            Duration leaseTtl
    ) {
        public ActiveExecution {
            if (executionId == null) {
                throw new IllegalArgumentException("executionId is required");
            }
            if (workerId == null || workerId.isBlank()) {
                throw new IllegalArgumentException("workerId is required");
            }
            if (leaseToken == null || leaseToken.isBlank()) {
                throw new IllegalArgumentException("leaseToken is required");
            }
            if (leaseTtl == null || leaseTtl.isNegative() || leaseTtl.isZero()) {
                throw new IllegalArgumentException("leaseTtl must be positive");
            }
        }
    }
}
