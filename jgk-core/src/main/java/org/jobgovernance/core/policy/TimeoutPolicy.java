package org.jobgovernance.core.policy;

import java.time.Duration;

public interface TimeoutPolicy {

    Duration queueWaitTimeout();

    Duration startDeadlineTimeout();

    Duration executionTimeout();

    Duration heartbeatTimeout();

    Duration leaseTtl();

    record DefaultTimeoutPolicy(
            Duration queueWaitTimeout,
            Duration startDeadlineTimeout,
            Duration executionTimeout,
            Duration heartbeatTimeout,
            Duration leaseTtl
    ) implements TimeoutPolicy {
        public DefaultTimeoutPolicy {
            queueWaitTimeout = queueWaitTimeout == null ? Duration.ofMinutes(30) : queueWaitTimeout;
            startDeadlineTimeout = startDeadlineTimeout == null ? Duration.ofMinutes(10) : startDeadlineTimeout;
            executionTimeout = executionTimeout == null ? Duration.ofMinutes(30) : executionTimeout;
            heartbeatTimeout = heartbeatTimeout == null ? Duration.ofMinutes(1) : heartbeatTimeout;
            leaseTtl = leaseTtl == null ? Duration.ofMinutes(2) : leaseTtl;
            validatePositive(queueWaitTimeout, "queueWaitTimeout");
            validatePositive(startDeadlineTimeout, "startDeadlineTimeout");
            validatePositive(executionTimeout, "executionTimeout");
            validatePositive(heartbeatTimeout, "heartbeatTimeout");
            validatePositive(leaseTtl, "leaseTtl");
            if (leaseTtl.compareTo(heartbeatTimeout) < 0) {
                throw new IllegalArgumentException("leaseTtl must be >= heartbeatTimeout");
            }
        }

        private static void validatePositive(Duration duration, String field) {
            if (duration.isZero() || duration.isNegative()) {
                throw new IllegalArgumentException(field + " must be positive");
            }
        }
    }
}
