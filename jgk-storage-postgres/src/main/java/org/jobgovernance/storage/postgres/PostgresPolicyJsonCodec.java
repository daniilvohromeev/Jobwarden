package org.jobgovernance.storage.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jobgovernance.core.api.ExecutionContext;
import org.jobgovernance.core.model.ConcurrencyPolicy;
import org.jobgovernance.core.model.JobPolicy;
import org.jobgovernance.core.model.MisfirePolicy;
import org.jobgovernance.core.policy.IdempotencyStrategy;
import org.jobgovernance.core.policy.RetryStrategies;
import org.jobgovernance.core.policy.RetryStrategy;
import org.jobgovernance.core.policy.TimeoutPolicy;

import java.time.Duration;
import java.util.Locale;
import java.util.Optional;

final class PostgresPolicyJsonCodec {

    private static final int DEFAULT_MAX_ATTEMPTS = 3;
    private static final Duration DEFAULT_RETRY_DELAY = Duration.ofSeconds(30);

    private final ObjectMapper objectMapper = new ObjectMapper();

    String encode(JobPolicy policy) {
        try {
            return objectMapper.writeValueAsString(PolicyDocument.from(policy));
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Failed to encode job policy JSON", exception);
        }
    }

    JobPolicy decode(String policyJson) {
        if (policyJson == null || policyJson.isBlank()) {
            return defaultPolicy();
        }
        try {
            PolicyDocument document = objectMapper.readValue(policyJson, PolicyDocument.class);
            return document.toDomain();
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Failed to decode job policy JSON", exception);
        }
    }

    private static JobPolicy defaultPolicy() {
        return new JobPolicy(
                RetryStrategies.fixedDelay(new RetryStrategies.FixedDelayConfig(DEFAULT_MAX_ATTEMPTS, DEFAULT_RETRY_DELAY)),
                new TimeoutPolicy.DefaultTimeoutPolicy(null, null, null, null, null),
                MisfirePolicy.CATCH_UP_LATEST_ONLY,
                new ConcurrencyPolicy.ForbidOverlap(),
                new PersistedIdempotencyStrategy("default"),
                false
        );
    }

    private record PolicyDocument(
            RetryDocument retry,
            TimeoutDocument timeout,
            String misfirePolicy,
            ConcurrencyDocument concurrency,
            boolean catchUpEnabled,
            IdempotencyDocument idempotency
    ) {
        static PolicyDocument from(JobPolicy policy) {
            int maxAttempts = policy.retryStrategy().maxAttemptsHint().orElse(DEFAULT_MAX_ATTEMPTS);
            RetryDocument retry = new RetryDocument(policy.retryStrategy().getClass().getName(), maxAttempts, DEFAULT_RETRY_DELAY.toMillis());
            TimeoutDocument timeout = TimeoutDocument.from(policy.timeoutPolicy());
            ConcurrencyDocument concurrency = ConcurrencyDocument.from(policy.concurrencyPolicy());
            IdempotencyDocument idempotency = new IdempotencyDocument(policy.idempotencyStrategy().getClass().getName());
            return new PolicyDocument(
                    retry,
                    timeout,
                    policy.misfirePolicy().name(),
                    concurrency,
                    policy.catchUpEnabled(),
                    idempotency
            );
        }

        JobPolicy toDomain() {
            RetryDocument retryDocument = retry == null ? new RetryDocument(null, DEFAULT_MAX_ATTEMPTS, DEFAULT_RETRY_DELAY.toMillis()) : retry;
            int maxAttempts = retryDocument.maxAttempts() < 1 ? DEFAULT_MAX_ATTEMPTS : retryDocument.maxAttempts();
            long retryDelayMs = retryDocument.retryDelayMs() < 1 ? DEFAULT_RETRY_DELAY.toMillis() : retryDocument.retryDelayMs();

            RetryStrategy retryStrategy = RetryStrategies.fixedDelay(
                    new RetryStrategies.FixedDelayConfig(maxAttempts, Duration.ofMillis(retryDelayMs))
            );
            TimeoutPolicy timeoutPolicy = timeout == null
                    ? new TimeoutPolicy.DefaultTimeoutPolicy(null, null, null, null, null)
                    : timeout.toDomain();
            MisfirePolicy misfire = parseMisfire(misfirePolicy);
            ConcurrencyPolicy concurrencyPolicy = concurrency == null
                    ? new ConcurrencyPolicy.ForbidOverlap()
                    : concurrency.toDomain();
            IdempotencyStrategy idempotencyStrategy = new PersistedIdempotencyStrategy(
                    idempotency == null ? "unknown" : idempotency.strategyClass()
            );
            return new JobPolicy(
                    retryStrategy,
                    timeoutPolicy,
                    misfire,
                    concurrencyPolicy,
                    idempotencyStrategy,
                    catchUpEnabled
            );
        }

        private MisfirePolicy parseMisfire(String value) {
            if (value == null || value.isBlank()) {
                return MisfirePolicy.CATCH_UP_LATEST_ONLY;
            }
            return MisfirePolicy.valueOf(value.trim().toUpperCase(Locale.ROOT));
        }
    }

    private record RetryDocument(
            String strategyClass,
            int maxAttempts,
            long retryDelayMs
    ) {
    }

    private record TimeoutDocument(
            long queueWaitTimeoutMs,
            long startDeadlineTimeoutMs,
            long executionTimeoutMs,
            long heartbeatTimeoutMs,
            long leaseTtlMs
    ) {
        static TimeoutDocument from(TimeoutPolicy policy) {
            return new TimeoutDocument(
                    policy.queueWaitTimeout().toMillis(),
                    policy.startDeadlineTimeout().toMillis(),
                    policy.executionTimeout().toMillis(),
                    policy.heartbeatTimeout().toMillis(),
                    policy.leaseTtl().toMillis()
            );
        }

        TimeoutPolicy toDomain() {
            return new TimeoutPolicy.DefaultTimeoutPolicy(
                    toDuration(queueWaitTimeoutMs, Duration.ofMinutes(30)),
                    toDuration(startDeadlineTimeoutMs, Duration.ofMinutes(10)),
                    toDuration(executionTimeoutMs, Duration.ofMinutes(30)),
                    toDuration(heartbeatTimeoutMs, Duration.ofMinutes(1)),
                    toDuration(leaseTtlMs, Duration.ofMinutes(2))
            );
        }

        private static Duration toDuration(long millis, Duration fallback) {
            return millis < 1 ? fallback : Duration.ofMillis(millis);
        }
    }

    private record ConcurrencyDocument(
            String kind,
            Integer limit
    ) {
        static ConcurrencyDocument from(ConcurrencyPolicy policy) {
            return switch (policy) {
                case ConcurrencyPolicy.ForbidOverlap ignored -> new ConcurrencyDocument(ConcurrencyPolicy.Kind.FORBID_OVERLAP.name(), null);
                case ConcurrencyPolicy.AllowOverlap ignored -> new ConcurrencyDocument(ConcurrencyPolicy.Kind.ALLOW_OVERLAP.name(), null);
                case ConcurrencyPolicy.AllowOverlapUpTo upTo -> new ConcurrencyDocument(ConcurrencyPolicy.Kind.ALLOW_OVERLAP_UP_TO.name(), upTo.maxParallelExecutions());
                case ConcurrencyPolicy.SingletonClusterWide ignored -> new ConcurrencyDocument(ConcurrencyPolicy.Kind.SINGLETON_CLUSTER_WIDE.name(), null);
                case ConcurrencyPolicy.SingletonPerTenant ignored -> new ConcurrencyDocument(ConcurrencyPolicy.Kind.SINGLETON_PER_TENANT.name(), null);
                case ConcurrencyPolicy.ShardByPartitionKey shard -> new ConcurrencyDocument(ConcurrencyPolicy.Kind.SHARD_BY_PARTITION_KEY.name(), shard.maxParallelPerShard());
            };
        }

        ConcurrencyPolicy toDomain() {
            if (kind == null || kind.isBlank()) {
                return new ConcurrencyPolicy.ForbidOverlap();
            }
            ConcurrencyPolicy.Kind parsedKind = ConcurrencyPolicy.Kind.valueOf(kind.trim().toUpperCase(Locale.ROOT));
            return switch (parsedKind) {
                case FORBID_OVERLAP -> new ConcurrencyPolicy.ForbidOverlap();
                case ALLOW_OVERLAP -> new ConcurrencyPolicy.AllowOverlap();
                case ALLOW_OVERLAP_UP_TO -> new ConcurrencyPolicy.AllowOverlapUpTo(limit == null || limit < 1 ? 1 : limit);
                case SINGLETON_CLUSTER_WIDE -> new ConcurrencyPolicy.SingletonClusterWide();
                case SINGLETON_PER_TENANT -> new ConcurrencyPolicy.SingletonPerTenant();
                case SHARD_BY_PARTITION_KEY -> new ConcurrencyPolicy.ShardByPartitionKey(limit == null || limit < 1 ? 1 : limit);
            };
        }
    }

    private record IdempotencyDocument(String strategyClass) {
    }

    private static final class PersistedIdempotencyStrategy implements IdempotencyStrategy {

        private final String strategyClass;

        private PersistedIdempotencyStrategy(String strategyClass) {
            this.strategyClass = strategyClass == null ? "unknown" : strategyClass;
        }

        @Override
        public Optional<String> deduplicationKey(ExecutionContext<?> context) {
            return Optional.empty();
        }

        @Override
        public BeforeExecutionDecision beforeExecution(ExecutionContext<?> context, String deduplicationKey) {
            return BeforeExecutionDecision.EXECUTE;
        }

        @Override
        public void afterExecution(ExecutionContext<?> context, String deduplicationKey, AfterExecutionResult result) {
        }

        @Override
        public String toString() {
            return "PersistedIdempotencyStrategy[" + strategyClass + "]";
        }
    }
}
