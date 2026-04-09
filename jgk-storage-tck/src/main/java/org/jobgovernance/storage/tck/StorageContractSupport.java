package org.jobgovernance.storage.tck;

import org.jobgovernance.core.api.ExecutionContext;
import org.jobgovernance.core.model.ConcurrencyPolicy;
import org.jobgovernance.core.model.ExecutionMode;
import org.jobgovernance.core.model.JobDefinition;
import org.jobgovernance.core.model.JobDefinitionState;
import org.jobgovernance.core.model.JobPolicy;
import org.jobgovernance.core.model.JobSchedule;
import org.jobgovernance.core.model.MisfirePolicy;
import org.jobgovernance.core.policy.IdempotencyStrategy;
import org.jobgovernance.core.policy.RetryStrategies;
import org.jobgovernance.core.policy.TimeoutPolicy;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public interface StorageContractSupport {

    void clearStorage();

    void seedDefinition(String jobKey);

    UUID seedScheduledExecution(String jobKey, Instant scheduledAt);

    default Instant fixedNow() {
        return Instant.parse("2026-01-01T00:00:00Z");
    }

    default JobDefinition sampleDefinition(String jobKey, JobDefinitionState state) {
        return new JobDefinition(
                jobKey,
                1,
                jobKey,
                "storage contract definition",
                "tck",
                Set.of("tck", "storage"),
                ExecutionMode.BLOCKING,
                new JobSchedule.FixedDelaySchedule(Duration.ofMinutes(5), Duration.ofSeconds(30), ZoneId.of("UTC"), null, null),
                new JobPolicy(
                        RetryStrategies.fixedDelay(new RetryStrategies.FixedDelayConfig(3, Duration.ofSeconds(10))),
                        new TimeoutPolicy.DefaultTimeoutPolicy(
                                Duration.ofMinutes(10),
                                Duration.ofMinutes(5),
                                Duration.ofMinutes(5),
                                Duration.ofSeconds(30),
                                Duration.ofMinutes(1)
                        ),
                        MisfirePolicy.CATCH_UP_LATEST_ONLY,
                        new ConcurrencyPolicy.AllowOverlap(),
                        new NoopIdempotencyStrategy(),
                        true
                ),
                "schema-v1",
                state,
                true,
                false,
                null
        );
    }

    final class NoopIdempotencyStrategy implements IdempotencyStrategy {
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
    }
}
