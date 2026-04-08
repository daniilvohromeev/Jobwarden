package org.jobgovernance.core.policy;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TimeoutPolicyTest {

    @Test
    void shouldApplyDefaultsWhenDurationsAreNull() {
        TimeoutPolicy.DefaultTimeoutPolicy policy = new TimeoutPolicy.DefaultTimeoutPolicy(
                null,
                null,
                null,
                null,
                null
        );

        assertEquals(Duration.ofMinutes(30), policy.queueWaitTimeout());
        assertEquals(Duration.ofMinutes(10), policy.startDeadlineTimeout());
        assertEquals(Duration.ofMinutes(30), policy.executionTimeout());
        assertEquals(Duration.ofMinutes(1), policy.heartbeatTimeout());
        assertEquals(Duration.ofMinutes(2), policy.leaseTtl());
    }

    @Test
    void shouldRejectNonPositiveDurations() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new TimeoutPolicy.DefaultTimeoutPolicy(
                        Duration.ZERO,
                        Duration.ofMinutes(1),
                        Duration.ofMinutes(1),
                        Duration.ofMinutes(1),
                        Duration.ofMinutes(2)
                )
        );
    }

    @Test
    void shouldRejectLeaseTtlSmallerThanHeartbeatTimeout() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new TimeoutPolicy.DefaultTimeoutPolicy(
                        Duration.ofMinutes(1),
                        Duration.ofMinutes(1),
                        Duration.ofMinutes(1),
                        Duration.ofMinutes(2),
                        Duration.ofMinutes(1)
                )
        );
    }
}
