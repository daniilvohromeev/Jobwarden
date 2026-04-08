package org.jobgovernance.core.model;

import org.jobgovernance.core.policy.IdempotencyStrategy;
import org.jobgovernance.core.policy.RetryStrategy;
import org.jobgovernance.core.policy.TimeoutPolicy;

public record JobPolicy(
        RetryStrategy retryStrategy,
        TimeoutPolicy timeoutPolicy,
        MisfirePolicy misfirePolicy,
        ConcurrencyPolicy concurrencyPolicy,
        IdempotencyStrategy idempotencyStrategy,
        boolean catchUpEnabled
) {
    public JobPolicy {
        if (retryStrategy == null) {
            throw new IllegalArgumentException("retryStrategy is required");
        }
        if (timeoutPolicy == null) {
            throw new IllegalArgumentException("timeoutPolicy is required");
        }
        if (misfirePolicy == null) {
            throw new IllegalArgumentException("misfirePolicy is required");
        }
        if (concurrencyPolicy == null) {
            throw new IllegalArgumentException("concurrencyPolicy is required");
        }
        if (idempotencyStrategy == null) {
            throw new IllegalArgumentException("idempotencyStrategy is required");
        }
    }
}
