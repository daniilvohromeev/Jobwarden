package org.jobgovernance.core.model;

import java.time.Instant;
import java.util.UUID;

public record JobExecution(
        UUID executionId,
        String jobKey,
        TriggerType triggerType,
        Instant scheduledAt,
        Instant claimedAt,
        Instant startedAt,
        Instant finishedAt,
        ExecutionStatus status,
        String workerId,
        int attempt,
        String payloadRef,
        String resultSummary,
        String errorSummary,
        boolean cancellationRequested,
        long fencingToken,
        String leaseToken,
        String correlationId,
        String traceId,
        String causationId,
        UUID parentExecutionId,
        String tenantId
) {
    public JobExecution {
        if (executionId == null) {
            throw new IllegalArgumentException("executionId is required");
        }
        if (jobKey == null || jobKey.isBlank()) {
            throw new IllegalArgumentException("jobKey is required");
        }
        if (triggerType == null) {
            throw new IllegalArgumentException("triggerType is required");
        }
        if (scheduledAt == null) {
            throw new IllegalArgumentException("scheduledAt is required");
        }
        if (status == null) {
            throw new IllegalArgumentException("status is required");
        }
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1");
        }
    }
}
