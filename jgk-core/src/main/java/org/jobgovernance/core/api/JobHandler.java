package org.jobgovernance.core.api;

@FunctionalInterface
public interface JobHandler<P, R> {

    R handle(ExecutionContext<P> context, CancellationToken cancellationToken) throws Exception;
}
