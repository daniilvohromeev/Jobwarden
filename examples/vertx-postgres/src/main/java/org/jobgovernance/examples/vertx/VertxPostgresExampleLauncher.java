package org.jobgovernance.examples.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
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
import org.jobgovernance.vertx.VertxJobAdapters;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class VertxPostgresExampleLauncher {

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new GovernanceSampleVerticle());
    }

    static class GovernanceSampleVerticle extends AbstractVerticle {

        @Override
        public void start(Promise<Void> startPromise) {
            AsyncJobHandler<Map<String, Object>, String> eventLoopSafe = VertxJobAdapters.eventLoopSafe(
                    vertx,
                    (context, token) -> Future.succeededFuture("event-loop result " + context.executionId())
            );

            AsyncJobHandler<Map<String, Object>, String> blocking = VertxJobAdapters.blocking(
                    vertx,
                    (context, token) -> {
                        token.throwIfCancellationRequested();
                        Thread.sleep(50);
                        return "blocking result " + context.executionId();
                    }
            );

            JobRegistry.JobRegistration<Map<String, Object>, String> eventLoopRegistration =
                    new JobRegistry.JobRegistration<>(
                            eventLoopJobDefinition(),
                            payloadClass(),
                            JobRegistry.HandlerType.ASYNC,
                            null,
                            eventLoopSafe
                    );

            JobRegistry.JobRegistration<Map<String, Object>, String> blockingRegistration =
                    new JobRegistry.JobRegistration<>(
                            blockingJobDefinition(),
                            payloadClass(),
                            JobRegistry.HandlerType.ASYNC,
                            null,
                            blocking
                    );

            if (eventLoopRegistration != null && blockingRegistration != null) {
                startPromise.complete();
            } else {
                startPromise.fail("Could not initialize registrations");
            }
        }

        @SuppressWarnings("unchecked")
        private Class<Map<String, Object>> payloadClass() {
            return (Class<Map<String, Object>>) (Class<?>) Map.class;
        }

        private JobDefinition eventLoopJobDefinition() {
            return new JobDefinition(
                    "inventory.emit-heartbeat",
                    1,
                    "Emit heartbeat",
                    "Small non-blocking job that can run on event loop",
                    "inventory-platform",
                    Set.of("inventory", "vertx", "non-blocking"),
                    ExecutionMode.VERTX_FUTURE,
                    new JobSchedule.FixedRateSchedule(Duration.ofSeconds(10), Duration.ofSeconds(1), ZoneId.of("UTC"), null, null),
                    defaultPolicy(new ConcurrencyPolicy.AllowOverlapUpTo(8)),
                    "heartbeat-v1",
                    JobDefinitionState.ENABLED,
                    true,
                    false,
                    null
            );
        }

        private JobDefinition blockingJobDefinition() {
            return new JobDefinition(
                    "inventory.rebuild-projection",
                    1,
                    "Rebuild projection",
                    "Blocking job routed through executeBlocking",
                    "inventory-platform",
                    Set.of("inventory", "vertx", "blocking"),
                    ExecutionMode.BLOCKING,
                    new JobSchedule.CronSchedule("0 */15 * * * *", ZoneId.of("UTC"), null, null),
                    defaultPolicy(new ConcurrencyPolicy.ForbidOverlap()),
                    "projection-v1",
                    JobDefinitionState.ENABLED,
                    true,
                    false,
                    null
            );
        }

        private JobPolicy defaultPolicy(ConcurrencyPolicy concurrencyPolicy) {
            RetryStrategy retryStrategy = (ctx, error) -> ctx.attempt() >= 3
                    ? RetryStrategy.RetryDecision.noRetry("max attempts reached")
                    : RetryStrategy.RetryDecision.retryAt(Instant.now().plusSeconds(5), "fixed-delay", "transient failure");

            TimeoutPolicy timeoutPolicy = new TimeoutPolicy.DefaultTimeoutPolicy(
                    Duration.ofMinutes(10),
                    Duration.ofMinutes(1),
                    Duration.ofMinutes(5),
                    Duration.ofSeconds(20),
                    Duration.ofSeconds(40)
            );

            IdempotencyStrategy idempotencyStrategy = new IdempotencyStrategy() {
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

            return new JobPolicy(
                    retryStrategy,
                    timeoutPolicy,
                    MisfirePolicy.CATCH_UP_LATEST_ONLY,
                    concurrencyPolicy,
                    idempotencyStrategy,
                    false
            );
        }
    }
}
