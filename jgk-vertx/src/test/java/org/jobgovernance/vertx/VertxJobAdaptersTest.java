package org.jobgovernance.vertx;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.jobgovernance.core.api.CancellationToken;
import org.jobgovernance.core.api.ExecutionContext;
import org.jobgovernance.core.model.TriggerType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VertxJobAdaptersTest {

    private Vertx vertx;

    @BeforeEach
    void setUp() {
        this.vertx = Vertx.vertx();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void eventLoopSafeShouldExecuteDelegateOnEventLoopThread() throws Exception {
        var handler = VertxJobAdapters.<String, String>eventLoopSafe(
                vertx,
                (context, token) -> Future.succeededFuture(Thread.currentThread().getName())
        );

        String threadName = handler.handleAsync(sampleContext("vertx.event-loop"), CancellationToken.none())
                .toCompletableFuture()
                .get(2, TimeUnit.SECONDS);

        assertTrue(threadName.contains("eventloop"));
    }

    @Test
    void eventLoopSafeShouldFailFastWhenCancelled() {
        AtomicBoolean delegateInvoked = new AtomicBoolean(false);
        var handler = VertxJobAdapters.<String, String>eventLoopSafe(
                vertx,
                (context, token) -> {
                    delegateInvoked.set(true);
                    return Future.succeededFuture("ok");
                }
        );

        ExecutionException exception = assertThrows(
                ExecutionException.class,
                () -> handler.handleAsync(sampleContext("vertx.cancelled"), () -> true).toCompletableFuture().get(2, TimeUnit.SECONDS)
        );

        assertTrue(exception.getCause() instanceof RuntimeException);
        assertFalse(delegateInvoked.get());
    }

    @Test
    void blockingShouldExecuteOffEventLoop() throws Exception {
        var handler = VertxJobAdapters.<String, String>blocking(
                vertx,
                (context, token) -> Thread.currentThread().getName()
        );

        String threadName = handler.handleAsync(sampleContext("vertx.blocking"), CancellationToken.none())
                .toCompletableFuture()
                .get(2, TimeUnit.SECONDS);

        assertFalse(threadName.contains("eventloop"));
    }

    @Test
    void blockingShouldPropagateCancellation() {
        var handler = VertxJobAdapters.<String, String>blocking(
                vertx,
                (context, token) -> {
                    token.throwIfCancellationRequested();
                    return "never";
                }
        );

        assertThrows(
                CancellationException.class,
                () -> handler.handleAsync(sampleContext("vertx.blocking.cancel"), () -> true).toCompletableFuture().get(2, TimeUnit.SECONDS)
        );
    }

    private static ExecutionContext<String> sampleContext(String jobKey) {
        return new ExecutionContext<>(
                UUID.randomUUID(),
                jobKey,
                1,
                TriggerType.MANUAL,
                Instant.parse("2026-01-01T00:00:00Z"),
                "worker-test",
                "lease-test",
                "corr-test",
                "trace-test",
                "cause-test",
                null,
                null,
                "{\"payload\":true}",
                Map.of("env", "test")
        );
    }
}
