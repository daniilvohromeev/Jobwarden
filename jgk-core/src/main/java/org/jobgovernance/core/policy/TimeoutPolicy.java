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
        }
    }
}
