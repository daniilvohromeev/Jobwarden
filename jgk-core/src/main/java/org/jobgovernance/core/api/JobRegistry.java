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
        }
    }

    enum HandlerType {
        SYNC,
        ASYNC
    }
}
