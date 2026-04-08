package org.jobgovernance.examples.webflux;

import org.jobgovernance.core.api.AsyncJobHandler;
import org.jobgovernance.core.api.JobRegistry;
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
import org.jobgovernance.reactor.ReactorJobAdapters;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Configuration
public class ReactorJobConfiguration {

    @Bean
    public JobRegistry.JobRegistration<Map<String, Object>, String> refreshCacheRegistration() {
        JobDefinition definition = new JobDefinition(
                "catalog.refresh-cache",
                1,
                "Refresh cache",
                "Warms product cache in reactor mode",
                "catalog-platform",
                Set.of("catalog", "reactive"),
                ExecutionMode.REACTOR,
                new JobSchedule.FixedRateSchedule(Duration.ofMinutes(5), Duration.ofSeconds(15), ZoneId.of("UTC"), null, null),
                new JobPolicy(
                        retryStrategy(),
                        timeoutPolicy(),
                        MisfirePolicy.RUN_ONCE_IMMEDIATELY,
                        new ConcurrencyPolicy.AllowOverlapUpTo(2),
                        idempotencyStrategy(),
                        false
                ),
                "cache-refresh-v1",
                JobDefinitionState.ENABLED,
                true,
                false,
                null
        );

        AsyncJobHandler<Map<String, Object>, String> asyncHandler = ReactorJobAdapters.monoHandler(
                (context, token) -> Mono.defer(() -> {
                    token.throwIfCancellationRequested();
                    return Mono.just("Refreshed cache for " + context.executionId());
                }),
                Schedulers.boundedElastic(),
                Duration.ofSeconds(20)
        );

        return new JobRegistry.JobRegistration<>(
                definition,
                payloadClass(),
                JobRegistry.HandlerType.ASYNC,
                null,
                asyncHandler
        );
    }

    @SuppressWarnings("unchecked")
    private Class<Map<String, Object>> payloadClass() {
        return (Class<Map<String, Object>>) (Class<?>) Map.class;
    }

    private RetryStrategy retryStrategy() {
        return (ctx, failure) -> ctx.attempt() >= 4
                ? RetryStrategy.RetryDecision.noRetry("retry budget exhausted")
                : RetryStrategy.RetryDecision.retryAt(Instant.now().plusSeconds(10L * ctx.attempt()), "backoff", "transient reactor failure");
    }

    private TimeoutPolicy timeoutPolicy() {
        return new TimeoutPolicy.DefaultTimeoutPolicy(
                Duration.ofMinutes(5),
                Duration.ofMinutes(2),
                Duration.ofSeconds(20),
                Duration.ofSeconds(15),
                Duration.ofMinutes(1)
        );
    }

    private IdempotencyStrategy idempotencyStrategy() {
        return new IdempotencyStrategy() {
            @Override
            public Optional<String> deduplicationKey(org.jobgovernance.core.api.ExecutionContext<?> context) {
                return Optional.of(context.jobKey() + ":" + context.scheduledAt());
            }

            @Override
            public BeforeExecutionDecision beforeExecution(org.jobgovernance.core.api.ExecutionContext<?> context, String deduplicationKey) {
                return BeforeExecutionDecision.EXECUTE;
            }

            @Override
            public void afterExecution(org.jobgovernance.core.api.ExecutionContext<?> context, String deduplicationKey, AfterExecutionResult result) {
            }
        };
    }
}
