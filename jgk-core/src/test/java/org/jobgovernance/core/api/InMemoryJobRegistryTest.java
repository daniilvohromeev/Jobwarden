package org.jobgovernance.core.api;

import org.jobgovernance.core.model.ConcurrencyPolicy;
import org.jobgovernance.core.model.ExecutionMode;
import org.jobgovernance.core.model.JobDefinition;
import org.jobgovernance.core.model.JobDefinitionState;
import org.jobgovernance.core.model.JobPolicy;
import org.jobgovernance.core.model.JobSchedule;
import org.jobgovernance.core.model.MisfirePolicy;
import org.jobgovernance.core.policy.IdempotencyStrategy;
import org.jobgovernance.core.policy.RetryStrategy;
import org.jobgovernance.core.policy.TimeoutPolicy;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryJobRegistryTest {

    @Test
    void shouldRegisterAndFindJob() {
        InMemoryJobRegistry registry = new InMemoryJobRegistry();
        JobRegistry.JobRegistration<Map<String, Object>, String> registration = syncRegistration("job-a", 1);

        registry.register(registration);

        Optional<JobRegistry.JobRegistration<?, ?>> found = registry.findByJobKey("job-a");
        assertTrue(found.isPresent());
        assertEquals(1, found.get().definition().version());
    }

    @Test
    void shouldReplaceRegistrationWithHigherVersion() {
        InMemoryJobRegistry registry = new InMemoryJobRegistry();
        registry.register(syncRegistration("job-a", 1));
        registry.register(syncRegistration("job-a", 2));

        assertEquals(2, registry.findByJobKey("job-a").orElseThrow().definition().version());
    }

    @Test
    void shouldRejectDuplicateOrOlderVersion() {
        InMemoryJobRegistry registry = new InMemoryJobRegistry();
        registry.register(syncRegistration("job-a", 2));

        assertThrows(IllegalStateException.class, () -> registry.register(syncRegistration("job-a", 2)));
        assertThrows(IllegalStateException.class, () -> registry.register(syncRegistration("job-a", 1)));
    }

    @Test
    void shouldValidateHandlerTypeAndHandlerPresence() {
        JobDefinition definition = definition("job-a", 1);

        assertThrows(IllegalArgumentException.class, () -> new JobRegistry.JobRegistration<>(
                definition,
                payloadClass(),
                JobRegistry.HandlerType.SYNC,
                null,
                null
        ));

        assertThrows(IllegalArgumentException.class, () -> new JobRegistry.JobRegistration<>(
                definition,
                payloadClass(),
                JobRegistry.HandlerType.ASYNC,
                null,
                null
        ));
    }

    private JobRegistry.JobRegistration<Map<String, Object>, String> syncRegistration(String jobKey, int version) {
        return new JobRegistry.JobRegistration<>(
                definition(jobKey, version),
                payloadClass(),
                JobRegistry.HandlerType.SYNC,
                (context, cancellationToken) -> "ok-" + context.executionId(),
                null
        );
    }

    @SuppressWarnings("unchecked")
    private Class<Map<String, Object>> payloadClass() {
        return (Class<Map<String, Object>>) (Class<?>) Map.class;
    }

    private JobDefinition definition(String jobKey, int version) {
        RetryStrategy retryStrategy = (context, failure) -> RetryStrategy.RetryDecision.noRetry("not retryable");
        TimeoutPolicy timeoutPolicy = new TimeoutPolicy.DefaultTimeoutPolicy(
                Duration.ofMinutes(1),
                Duration.ofMinutes(1),
                Duration.ofMinutes(1),
                Duration.ofSeconds(10),
                Duration.ofSeconds(30)
        );
        IdempotencyStrategy idempotencyStrategy = new IdempotencyStrategy() {
            @Override
            public Optional<String> deduplicationKey(ExecutionContext<?> context) {
                return Optional.of(context.jobKey() + ":" + context.scheduledAt());
            }

            @Override
            public BeforeExecutionDecision beforeExecution(ExecutionContext<?> context, String deduplicationKey) {
                return BeforeExecutionDecision.EXECUTE;
            }

            @Override
            public void afterExecution(ExecutionContext<?> context, String deduplicationKey, AfterExecutionResult result) {
            }
        };

        return new JobDefinition(
                jobKey,
                version,
                jobKey,
                "test job",
                "team",
                Set.of("test"),
                ExecutionMode.BLOCKING,
                new JobSchedule.FixedRateSchedule(Duration.ofMinutes(5), Duration.ZERO, ZoneId.of("UTC"), Instant.EPOCH, null),
                new JobPolicy(
                        retryStrategy,
                        timeoutPolicy,
                        MisfirePolicy.IGNORE,
                        new ConcurrencyPolicy.ForbidOverlap(),
                        idempotencyStrategy,
                        false
                ),
                "schema-v1",
                JobDefinitionState.ENABLED,
                true,
                false,
                null
        );
    }
}
