package org.jobgovernance.core.api;

import org.jobgovernance.core.model.TriggerType;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record ExecutionContext<P>(
        UUID executionId,
        String jobKey,
        int attempt,
        TriggerType triggerType,
        Instant scheduledAt,
        String workerId,
        String leaseToken,
        String correlationId,
        String traceId,
        String causationId,
        UUID parentExecutionId,
        String tenantId,
        P payload,
        Map<String, String> attributes
) {
    public ExecutionContext {
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    }
}
