package org.jobgovernance.core.policy;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

public final class RetryStrategies {

    private RetryStrategies() {
    }

    public static RetryStrategy neverRetry() {
        return (context, failure) -> RetryStrategy.RetryDecision.noRetry("retry disabled");
    }

    public static RetryStrategy fixedDelay(FixedDelayConfig config) {
        return new FixedDelayRetryStrategy(config);
    }

    public static RetryStrategy exponentialBackoff(ExponentialBackoffConfig config) {
        return new ExponentialRetryStrategy(config);
    }

    public record FixedDelayConfig(
            int maxAttempts,
            Duration delay,
            Duration retryWindow,
            double jitterFactor,
            Set<Class<? extends Throwable>> retryableExceptions,
            Set<Class<? extends Throwable>> nonRetryableExceptions,
            DoubleSupplier jitterRandomSource
    ) {
        public FixedDelayConfig {
            if (maxAttempts < 1) {
                throw new IllegalArgumentException("maxAttempts must be >= 1");
            }
            if (delay == null || delay.isNegative() || delay.isZero()) {
                throw new IllegalArgumentException("delay must be positive");
            }
            if (retryWindow != null && (retryWindow.isNegative() || retryWindow.isZero())) {
                throw new IllegalArgumentException("retryWindow must be positive when provided");
            }
            if (jitterFactor < 0.0 || jitterFactor > 1.0) {
                throw new IllegalArgumentException("jitterFactor must be in range [0, 1]");
            }
            retryableExceptions = retryableExceptions == null ? Set.of() : Set.copyOf(retryableExceptions);
            nonRetryableExceptions = nonRetryableExceptions == null ? Set.of() : Set.copyOf(nonRetryableExceptions);
            jitterRandomSource = jitterRandomSource == null ? ThreadLocalRandom.current()::nextDouble : jitterRandomSource;
        }

        public FixedDelayConfig(int maxAttempts, Duration delay) {
            this(maxAttempts, delay, null, 0.0, Set.of(), Set.of(), null);
        }
    }

    public record ExponentialBackoffConfig(
            int maxAttempts,
            Duration initialDelay,
            Duration maxDelay,
            double multiplier,
            Duration retryWindow,
            double jitterFactor,
            Set<Class<? extends Throwable>> retryableExceptions,
            Set<Class<? extends Throwable>> nonRetryableExceptions,
            DoubleSupplier jitterRandomSource
    ) {
        public ExponentialBackoffConfig {
            if (maxAttempts < 1) {
                throw new IllegalArgumentException("maxAttempts must be >= 1");
            }
            if (initialDelay == null || initialDelay.isNegative() || initialDelay.isZero()) {
                throw new IllegalArgumentException("initialDelay must be positive");
            }
            if (maxDelay == null || maxDelay.isNegative() || maxDelay.isZero()) {
                throw new IllegalArgumentException("maxDelay must be positive");
            }
            if (maxDelay.compareTo(initialDelay) < 0) {
                throw new IllegalArgumentException("maxDelay must be >= initialDelay");
            }
            if (multiplier < 1.0) {
                throw new IllegalArgumentException("multiplier must be >= 1.0");
            }
            if (retryWindow != null && (retryWindow.isNegative() || retryWindow.isZero())) {
                throw new IllegalArgumentException("retryWindow must be positive when provided");
            }
            if (jitterFactor < 0.0 || jitterFactor > 1.0) {
                throw new IllegalArgumentException("jitterFactor must be in range [0, 1]");
            }
            retryableExceptions = retryableExceptions == null ? Set.of() : Set.copyOf(retryableExceptions);
            nonRetryableExceptions = nonRetryableExceptions == null ? Set.of() : Set.copyOf(nonRetryableExceptions);
            jitterRandomSource = jitterRandomSource == null ? ThreadLocalRandom.current()::nextDouble : jitterRandomSource;
        }

        public ExponentialBackoffConfig(int maxAttempts, Duration initialDelay, Duration maxDelay, double multiplier) {
            this(maxAttempts, initialDelay, maxDelay, multiplier, null, 0.0, Set.of(), Set.of(), null);
        }
    }

    private record FixedDelayRetryStrategy(FixedDelayConfig config) implements RetryStrategy {
        @Override
        public RetryDecision nextRetry(RetryContext context, Throwable failure) {
            return decide(
                    context,
                    failure,
                    config.maxAttempts(),
                    config.retryWindow(),
                    config.retryableExceptions(),
                    config.nonRetryableExceptions(),
                    config.delay().toMillis(),
                    "fixed-delay",
                    config.jitterFactor(),
                    config.jitterRandomSource()
            );
        }

        @Override
        public java.util.OptionalInt maxAttemptsHint() {
            return java.util.OptionalInt.of(config.maxAttempts());
        }
    }

    private record ExponentialRetryStrategy(ExponentialBackoffConfig config) implements RetryStrategy {
        @Override
        public RetryDecision nextRetry(RetryContext context, Throwable failure) {
            long nextDelayMillis = exponentialDelayMillis(
                    config.initialDelay(),
                    config.maxDelay(),
                    config.multiplier(),
                    context.attempt()
            );
            return decide(
                    context,
                    failure,
                    config.maxAttempts(),
                    config.retryWindow(),
                    config.retryableExceptions(),
                    config.nonRetryableExceptions(),
                    nextDelayMillis,
                    "exponential-backoff",
                    config.jitterFactor(),
                    config.jitterRandomSource()
            );
        }

        @Override
        public java.util.OptionalInt maxAttemptsHint() {
            return java.util.OptionalInt.of(config.maxAttempts());
        }
    }

    private static RetryStrategy.RetryDecision decide(
            RetryStrategy.RetryContext context,
            Throwable failure,
            int maxAttempts,
            Duration retryWindow,
            Set<Class<? extends Throwable>> retryableExceptions,
            Set<Class<? extends Throwable>> nonRetryableExceptions,
            long baseDelayMillis,
            String classification,
            double jitterFactor,
            DoubleSupplier jitterRandomSource
    ) {
        if (context == null) {
            throw new IllegalArgumentException("context is required");
        }
        Instant now = context.now();
        if (now == null) {
            throw new IllegalArgumentException("context.now is required");
        }
        if (context.attempt() >= maxAttempts) {
            return new RetryStrategy.RetryDecision(false, null, "max-attempts-exhausted", "max attempts reached");
        }
        if (isExceptionClassifiedNonRetryable(failure, nonRetryableExceptions)) {
            return new RetryStrategy.RetryDecision(false, null, "non-retryable-exception", "exception is non-retryable");
        }
        if (!retryableExceptions.isEmpty() && !isExceptionClassifiedRetryable(failure, retryableExceptions)) {
            return new RetryStrategy.RetryDecision(false, null, "exception-not-retryable", "exception not in retryable set");
        }

        long delayWithJitter = applyJitter(baseDelayMillis, jitterFactor, jitterRandomSource);
        Instant nextAttemptAt = now.plusMillis(delayWithJitter);
        if (retryWindow != null && context.firstAttemptAt() != null) {
            Instant windowEnd = context.firstAttemptAt().plus(retryWindow);
            if (now.isAfter(windowEnd) || nextAttemptAt.isAfter(windowEnd)) {
                return new RetryStrategy.RetryDecision(false, null, "retry-window-exhausted", "retry window exceeded");
            }
        }
        return RetryStrategy.RetryDecision.retryAt(nextAttemptAt, classification, "retry scheduled");
    }

    private static long exponentialDelayMillis(
            Duration initialDelay,
            Duration maxDelay,
            double multiplier,
            int currentAttempt
    ) {
        int exponent = Math.max(0, currentAttempt - 1);
        double calculated = initialDelay.toMillis() * Math.pow(multiplier, exponent);
        double bounded = Math.min(calculated, maxDelay.toMillis());
        return Math.max(1L, (long) Math.ceil(bounded));
    }

    private static long applyJitter(long baseDelayMillis, double jitterFactor, DoubleSupplier jitterRandomSource) {
        if (jitterFactor == 0.0) {
            return Math.max(1L, baseDelayMillis);
        }
        double random01 = jitterRandomSource.getAsDouble();
        double randomSigned = (random01 * 2.0) - 1.0;
        long jitter = (long) Math.round(baseDelayMillis * jitterFactor * randomSigned);
        return Math.max(1L, baseDelayMillis + jitter);
    }

    private static boolean isExceptionClassifiedNonRetryable(
            Throwable failure,
            Set<Class<? extends Throwable>> nonRetryableExceptions
    ) {
        return failure != null && matchesHierarchy(failure.getClass(), nonRetryableExceptions);
    }

    private static boolean isExceptionClassifiedRetryable(
            Throwable failure,
            Set<Class<? extends Throwable>> retryableExceptions
    ) {
        return failure != null && matchesHierarchy(failure.getClass(), retryableExceptions);
    }

    private static boolean matchesHierarchy(
            Class<? extends Throwable> failureType,
            Set<Class<? extends Throwable>> configuredTypes
    ) {
        return configuredTypes.stream().anyMatch(configured -> configured.isAssignableFrom(failureType));
    }
}
