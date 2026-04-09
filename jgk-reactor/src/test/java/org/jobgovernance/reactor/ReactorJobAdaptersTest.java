package org.jobgovernance.reactor;

import org.jobgovernance.core.api.CancellationToken;
import org.jobgovernance.core.api.ExecutionContext;
import org.jobgovernance.core.model.TriggerType;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReactorJobAdaptersTest {

    @Test
    void monoHandlerShouldRunAndExposeExecutionContext() throws Exception {
        ExecutionContext<String> context = sampleContext("reactor.mono");
        var executionThread = new AtomicReference<String>();

        var handler = ReactorJobAdapters.<String, String>monoHandler(
                (ctx, token) -> Mono.deferContextual(view -> Mono.fromCallable(() -> {
                    executionThread.set(Thread.currentThread().getName());
                    return view.get("jgk.executionId");
                })),
                Schedulers.boundedElastic(),
                Duration.ofSeconds(1)
        );

        String result = handler.handleAsync(context, CancellationToken.none())
                .toCompletableFuture()
                .get(2, TimeUnit.SECONDS);

        assertEquals(context.executionId().toString(), result);
        assertNotNull(executionThread.get());
    }

    @Test
    void monoHandlerShouldFailOnTimeout() {
        var handler = ReactorJobAdapters.<String, String>monoHandler(
                (ctx, token) -> Mono.<String>never(),
                Schedulers.parallel(),
                Duration.ofMillis(50)
        );

        ExecutionException exception = assertThrows(
                ExecutionException.class,
                () -> handler.handleAsync(sampleContext("reactor.timeout"), CancellationToken.none()).toCompletableFuture().get(1, TimeUnit.SECONDS)
        );

        assertTrue(exception.getCause() instanceof TimeoutException);
    }

    @Test
    void fluxHandlerShouldCollectValues() throws Exception {
        var handler = ReactorJobAdapters.<String, String>fluxHandler(
                (ctx, token) -> Flux.just("one", "two", "three"),
                Schedulers.immediate(),
                Duration.ofSeconds(1)
        );

        List<String> result = handler.handleAsync(sampleContext("reactor.flux"), CancellationToken.none())
                .toCompletableFuture()
                .get(2, TimeUnit.SECONDS);

        assertEquals(List.of("one", "two", "three"), result);
    }

    @Test
    void cancelAwareShouldFailWhenCancellationRequested() {
        CancellationToken cancelled = () -> true;
        ExecutionException exception = assertThrows(
                ExecutionException.class,
                () -> ReactorJobAdapters.cancelAware(Mono.just("ok"), cancelled).toCompletableFuture().get(1, TimeUnit.SECONDS)
        );

        assertTrue(exception.getCause() instanceof RuntimeException);
        assertTrue(exception.getCause().getMessage().contains("Cancellation requested"));
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
