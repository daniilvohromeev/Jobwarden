package org.jobgovernance.storage.spi;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface ManualTriggerRequestRepository {

    UUID enqueue(ManualTriggerRequest request);

    List<ManualTriggerRequest> fetchPending(int batchSize, Instant now);

    boolean markProcessed(UUID requestId, Instant processedAt, UUID executionId);

    record ManualTriggerRequest(
            UUID requestId,
            String jobKey,
            String tenantId,
            String payloadRef,
            String idempotencyKey,
            Instant triggerAt,
            String actor,
            Instant requestedAt
    ) {
    }
}
