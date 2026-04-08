package org.jobgovernance.core.policy;

import java.time.Instant;
import java.util.OptionalInt;

public interface RetryStrategy {

    RetryDecision nextRetry(RetryContext context, Throwable failure);

    default OptionalInt maxAttemptsHint() {
        return OptionalInt.empty();
    }

    record RetryContext(
            int attempt,
            Instant firstAttemptAt,
            Instant lastFailureAt,
            Instant now,
            String jobKey,
            String executionId
    ) {
    }

    record RetryDecision(
            boolean retryable,
            Instant nextAttemptAt,
            String classification,
            String reason
    ) {
        public static RetryDecision noRetry(String reason) {
            return new RetryDecision(false, null, "non-retryable", reason);
        }

        public static RetryDecision retryAt(Instant at, String classification, String reason) {
            return new RetryDecision(true, at, classification, reason);
        }
    }
}
