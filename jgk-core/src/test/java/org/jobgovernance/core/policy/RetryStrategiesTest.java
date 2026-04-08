package org.jobgovernance.core.policy;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryStrategiesTest {

    @Test
    void fixedDelayShouldRetryBeforeMaxAttempts() {
        RetryStrategy strategy = RetryStrategies.fixedDelay(new RetryStrategies.FixedDelayConfig(3, Duration.ofSeconds(10)));
        Instant now = Instant.parse("2026-01-01T00:00:00Z");

        RetryStrategy.RetryDecision decision = strategy.nextRetry(
                context(1, now.minusSeconds(5), now),
                new IllegalStateException("transient")
        );

        assertTrue(decision.retryable());
        assertEquals("fixed-delay", decision.classification());
        assertEquals(now.plusSeconds(10), decision.nextAttemptAt());
    }

    @Test
    void fixedDelayShouldStopWhenMaxAttemptsReached() {
        RetryStrategy strategy = RetryStrategies.fixedDelay(new RetryStrategies.FixedDelayConfig(2, Duration.ofSeconds(5)));
        Instant now = Instant.parse("2026-01-01T00:00:00Z");

        RetryStrategy.RetryDecision decision = strategy.nextRetry(
                context(2, now.minusSeconds(10), now),
                new RuntimeException("boom")
        );

        assertFalse(decision.retryable());
        assertEquals("max-attempts-exhausted", decision.classification());
        assertNull(decision.nextAttemptAt());
    }

    @Test
    void fixedDelayShouldRespectExceptionClassification() {
        RetryStrategy strategy = RetryStrategies.fixedDelay(
                new RetryStrategies.FixedDelayConfig(
                        5,
                        Duration.ofSeconds(5),
                        null,
                        0.0,
                        Set.of(IllegalStateException.class),
                        Set.of(IllegalArgumentException.class),
                        null
                )
        );
        Instant now = Instant.parse("2026-01-01T00:00:00Z");

        RetryStrategy.RetryDecision nonRetryable = strategy.nextRetry(
                context(1, now.minusSeconds(10), now),
                new IllegalArgumentException("bad request")
        );
        RetryStrategy.RetryDecision notInRetryableSet = strategy.nextRetry(
                context(1, now.minusSeconds(10), now),
                new RuntimeException("other")
        );
        RetryStrategy.RetryDecision retryable = strategy.nextRetry(
                context(1, now.minusSeconds(10), now),
                new IllegalStateException("transient")
        );

        assertFalse(nonRetryable.retryable());
        assertEquals("non-retryable-exception", nonRetryable.classification());

        assertFalse(notInRetryableSet.retryable());
        assertEquals("exception-not-retryable", notInRetryableSet.classification());

        assertTrue(retryable.retryable());
        assertEquals("fixed-delay", retryable.classification());
    }

    @Test
    void fixedDelayShouldRespectRetryWindow() {
        Instant firstAttempt = Instant.parse("2026-01-01T00:00:00Z");
        Instant now = firstAttempt.plusSeconds(15);
        RetryStrategy strategy = RetryStrategies.fixedDelay(
                new RetryStrategies.FixedDelayConfig(
                        10,
                        Duration.ofSeconds(10),
                        Duration.ofSeconds(20),
                        0.0,
                        Set.of(),
                        Set.of(),
                        null
                )
        );

        RetryStrategy.RetryDecision decision = strategy.nextRetry(
                context(2, firstAttempt, now),
                new RuntimeException("still failing")
        );

        assertFalse(decision.retryable());
        assertEquals("retry-window-exhausted", decision.classification());
    }

    @Test
    void exponentialBackoffShouldGrowAndCapDelay() {
        RetryStrategy strategy = RetryStrategies.exponentialBackoff(
                new RetryStrategies.ExponentialBackoffConfig(6, Duration.ofMillis(100), Duration.ofMillis(250), 2.0)
        );
        Instant now = Instant.parse("2026-01-01T00:00:00Z");

        RetryStrategy.RetryDecision first = strategy.nextRetry(context(1, now, now), new RuntimeException("fail"));
        RetryStrategy.RetryDecision second = strategy.nextRetry(context(2, now, now), new RuntimeException("fail"));
        RetryStrategy.RetryDecision third = strategy.nextRetry(context(3, now, now), new RuntimeException("fail"));

        assertEquals(now.plusMillis(100), first.nextAttemptAt());
        assertEquals(now.plusMillis(200), second.nextAttemptAt());
        assertEquals(now.plusMillis(250), third.nextAttemptAt());
        assertEquals("exponential-backoff", third.classification());
    }

    @Test
    void fixedDelayShouldApplyJitterWhenConfigured() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        RetryStrategy strategy = RetryStrategies.fixedDelay(
                new RetryStrategies.FixedDelayConfig(
                        5,
                        Duration.ofMillis(100),
                        null,
                        0.5,
                        Set.of(),
                        Set.of(),
                        () -> 1.0
                )
        );

        RetryStrategy.RetryDecision decision = strategy.nextRetry(context(1, now, now), new RuntimeException("boom"));

        assertTrue(decision.retryable());
        assertNotNull(decision.nextAttemptAt());
        assertEquals(now.plusMillis(150), decision.nextAttemptAt());
    }

    private static RetryStrategy.RetryContext context(int attempt, Instant firstAttemptAt, Instant now) {
        return new RetryStrategy.RetryContext(
                attempt,
                firstAttemptAt,
                now,
                now,
                "job-a",
                "exec-1"
        );
    }
}
