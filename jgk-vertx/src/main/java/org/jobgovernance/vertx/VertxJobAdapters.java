package org.jobgovernance.vertx;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.jobgovernance.core.api.AsyncJobHandler;
import org.jobgovernance.core.api.CancellationToken;
import org.jobgovernance.core.api.ExecutionContext;
import org.jobgovernance.core.api.JobHandler;

import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

public final class VertxJobAdapters {

    private VertxJobAdapters() {
    }

    public static <P, R> AsyncJobHandler<P, R> eventLoopSafe(
            Vertx vertx,
            BiFunction<ExecutionContext<P>, CancellationToken, Future<R>> delegate
    ) {
        return (context, cancellationToken) -> {
            Promise<R> promise = Promise.promise();
            vertx.runOnContext(ignored -> {
                if (cancellationToken.isCancellationRequested()) {
                    promise.fail("Cancellation requested");
                    return;
                }
                delegate.apply(context, cancellationToken).onComplete(promise);
            });
            return promise.future().toCompletionStage();
        };
    }

    public static <P, R> AsyncJobHandler<P, R> blocking(
            Vertx vertx,
            JobHandler<P, R> blockingHandler
    ) {
        return (context, cancellationToken) -> vertx
                .executeBlocking(() -> {
                    cancellationToken.throwIfCancellationRequested();
                    return blockingHandler.handle(context, cancellationToken);
                })
                .toCompletionStage();
    }

    public static <P, R> CompletionStage<R> toCompletionStage(Future<R> future) {
        return future.toCompletionStage();
    }
}
