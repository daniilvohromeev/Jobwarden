package org.jobgovernance.reactor;

import org.jobgovernance.core.api.AsyncJobHandler;
import org.jobgovernance.core.api.CancellationToken;
import org.jobgovernance.core.api.ExecutionContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

public final class ReactorJobAdapters {

    private ReactorJobAdapters() {
    }

    public static <P, R> AsyncJobHandler<P, R> monoHandler(
            BiFunction<ExecutionContext<P>, CancellationToken, Mono<R>> delegate,
            Scheduler scheduler,
            Duration timeout
    ) {
        return (context, cancellationToken) -> delegate.apply(context, cancellationToken)
                .publishOn(scheduler)
                .timeout(timeout)
                .contextWrite(current -> current.put("jgk.executionId", context.executionId().toString()))
                .toFuture();
    }

    public static <P, R> AsyncJobHandler<P, List<R>> fluxHandler(
            BiFunction<ExecutionContext<P>, CancellationToken, Flux<R>> delegate,
            Scheduler scheduler,
            Duration timeout
    ) {
        return (context, cancellationToken) -> delegate.apply(context, cancellationToken)
                .publishOn(scheduler)
                .timeout(timeout)
                .contextWrite(current -> current.put("jgk.executionId", context.executionId().toString()))
                .collectList()
                .toFuture();
    }

    public static <P, R> CompletionStage<R> cancelAware(
            Mono<R> mono,
            CancellationToken token
    ) {
        if (token.isCancellationRequested()) {
            return CompletableFuture.failedFuture(new RuntimeException("Cancellation requested before run"));
        }
        return mono.toFuture();
    }
}
