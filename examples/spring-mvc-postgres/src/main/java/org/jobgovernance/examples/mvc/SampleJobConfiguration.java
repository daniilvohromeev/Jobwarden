package org.jobgovernance.examples.mvc;

import org.jobgovernance.core.api.CancellationToken;
import org.jobgovernance.core.api.ExecutionContext;
import org.jobgovernance.core.api.JobHandler;
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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Configuration
public class SampleJobConfiguration {

    @Bean
    public JobRegistry.JobRegistration<Map<String, Object>, String> reconcileInvoicesRegistration() {
        JobDefinition definition = new JobDefinition(
                "billing.reconcile-invoices",
                1,
                "Reconcile invoices",
                "Nightly reconciliation with ERP",
                "billing-platform",
                Set.of("billing", "nightly"),
                ExecutionMode.BLOCKING,
                new JobSchedule.CronSchedule("0 5 * * *", ZoneId.of("UTC"), null, null),
                new JobPolicy(
                        retryStrategy(),
                        timeoutPolicy(),
                        MisfirePolicy.CATCH_UP_LATEST_ONLY,
                        new ConcurrencyPolicy.ForbidOverlap(),
                        idempotencyStrategy(),
                        true
                ),
                "invoice-reconcile-v1",
                JobDefinitionState.ENABLED,
                true,
                false,
                null
        );

        JobHandler<Map<String, Object>, String> handler = this::reconcileInvoices;

        return new JobRegistry.JobRegistration<>(
                definition,
                payloadClass(),
                JobRegistry.HandlerType.SYNC,
                handler,
                null
        );
    }

    @SuppressWarnings("unchecked")
    private Class<Map<String, Object>> payloadClass() {
        return (Class<Map<String, Object>>) (Class<?>) Map.class;
    }

    private String reconcileInvoices(ExecutionContext<Map<String, Object>> context, CancellationToken cancellationToken) {
        cancellationToken.throwIfCancellationRequested();
        return "Reconciled for execution " + context.executionId();
    }

    private RetryStrategy retryStrategy() {
        return (ctx, failure) -> {
            if (ctx.attempt() >= 5) {
                return RetryStrategy.RetryDecision.noRetry("max attempts reached");
            }
            return RetryStrategy.RetryDecision.retryAt(Instant.now().plusSeconds(30), "exponential", "transient failure");
        };
    }

    private TimeoutPolicy timeoutPolicy() {
        return new TimeoutPolicy.DefaultTimeoutPolicy(
                Duration.ofMinutes(15),
                Duration.ofMinutes(5),
                Duration.ofMinutes(10),
                Duration.ofSeconds(30),
                Duration.ofMinutes(2)
        );
    }

    private IdempotencyStrategy idempotencyStrategy() {
        return new IdempotencyStrategy() {
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
    }
}
