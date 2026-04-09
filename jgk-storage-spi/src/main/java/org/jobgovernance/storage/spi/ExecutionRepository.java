package org.jobgovernance.storage.spi;

import org.jobgovernance.core.model.JobExecution;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface ExecutionRepository {

    List<UUID> findDueExecutionIds(Instant now, int batchSize);

    Optional<JobExecution> claimExecution(UUID executionId, ClaimRequest request);

    boolean enqueueScheduledExecution(ScheduledExecutionInsert request, Instant createdAt);

    Optional<JobExecution> findExecution(UUID executionId);

    List<JobExecution> findExecutions(String jobKey, String tenantId, int limit);

    Optional<JobExecution> findByIdempotencyKey(String jobKey, String tenantId, String idempotencyKey);

    default List<WorkerStatus> findActiveWorkers(Instant now, int limit) {
        return List.of();
    }

    boolean markRunning(UUID executionId, String workerId, String leaseToken, Instant startedAt);

    boolean markSucceeded(UUID executionId, String workerId, String leaseToken, String resultSummary, Instant finishedAt);

    boolean markFailedRetryable(
            UUID executionId,
            String workerId,
            String leaseToken,
            String errorClass,
            String errorSummary,
            Instant nextRetryAt,
            Instant finishedAt
    );

    boolean markFailedFinal(
            UUID executionId,
            String workerId,
            String leaseToken,
            String errorClass,
            String errorSummary,
            Instant finishedAt
    );

    boolean markTimedOut(UUID executionId, String workerId, String leaseToken, String reason, Instant finishedAt);

    boolean markSkipped(UUID executionId, String workerId, String leaseToken, String reason, Instant finishedAt);

    boolean requestCancellation(UUID executionId, String actor, String reason, Instant requestedAt);

    boolean markCancelled(UUID executionId, String workerId, String leaseToken, String reason, Instant finishedAt);

    boolean renewLease(UUID executionId, String workerId, String leaseToken, Instant leaseExpiresAt, Instant heartbeatAt);

    int requeueRetryableExecutions(Instant retryDueAt, int batchSize, Instant now);

    int recoverStaleClaims(Instant leaseExpiredBefore, String recoveryWorkerId, Instant now);

    int markDeadExecutions(Instant deadline, String reason, Instant now);

    default int cleanupFinishedExecutions(Instant finishedBefore, int batchSize, Instant now) {
        return 0;
    }

    record ClaimRequest(
            String workerId,
            Instant claimedAt,
            Instant leaseExpiresAt,
            String leaseToken
    ) {
    }

    record ScheduledExecutionInsert(
            String jobKey,
            String tenantId,
            String triggerType,
            Instant scheduledAt,
            Instant claimableAt,
            int maxAttempts,
            String payloadRef,
            String dedupeKey,
            String correlationId,
            String traceId,
            String causationId,
            UUID parentExecutionId,
            String idempotencyKey,
            String businessKey
    ) {
    }

    record WorkerStatus(
            String workerId,
            int activeExecutions,
            Instant oldestClaimedAt,
            Instant lastHeartbeatAt,
            Instant leaseExpiresAt
    ) {
    }
}
