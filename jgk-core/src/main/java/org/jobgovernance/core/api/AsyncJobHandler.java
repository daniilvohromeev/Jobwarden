package org.jobgovernance.core.api;

import java.util.concurrent.CompletionStage;

@FunctionalInterface
public interface AsyncJobHandler<P, R> {

    CompletionStage<R> handleAsync(ExecutionContext<P> context, CancellationToken cancellationToken);
}
