package org.jobgovernance.core.api;

import org.jobgovernance.core.model.JobDefinition;

import java.util.Collection;
import java.util.Optional;

public interface JobRegistry {

    <P, R> void register(JobRegistration<P, R> registration);

    Optional<JobRegistration<?, ?>> findByJobKey(String jobKey);

    Collection<JobRegistration<?, ?>> all();

    record JobRegistration<P, R>(
            JobDefinition definition,
            Class<P> payloadType,
            HandlerType handlerType,
            JobHandler<P, R> syncHandler,
            AsyncJobHandler<P, R> asyncHandler
    ) {
        public JobRegistration {
            if (definition == null) {
                throw new IllegalArgumentException("definition is required");
            }
            if (payloadType == null) {
                throw new IllegalArgumentException("payloadType is required");
            }
            if (handlerType == null) {
                throw new IllegalArgumentException("handlerType is required");
            }
            if (handlerType == HandlerType.SYNC && syncHandler == null) {
                throw new IllegalArgumentException("syncHandler is required for SYNC handlerType");
            }
            if (handlerType == HandlerType.ASYNC && asyncHandler == null) {
                throw new IllegalArgumentException("asyncHandler is required for ASYNC handlerType");
            }
        }
    }

    enum HandlerType {
        SYNC,
        ASYNC
    }
}
