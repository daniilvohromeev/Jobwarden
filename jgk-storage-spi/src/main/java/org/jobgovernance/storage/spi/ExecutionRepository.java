package org.jobgovernance.storage.spi;

import org.jobgovernance.core.model.JobExecution;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface ExecutionRepository {

    List<UUID> findDueExecutionIds(Instant now, int batchSize);

    Optional<JobExecution> claimExecution(UUID executionId, ClaimRequest request);

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

    boolean requestCancellation(UUID executionId, String actor, String reason, Instant requestedAt);

    boolean markCancelled(UUID executionId, String workerId, String leaseToken, String reason, Instant finishedAt);

    boolean renewLease(UUID executionId, String workerId, String leaseToken, Instant leaseExpiresAt, Instant heartbeatAt);

    int requeueRetryableExecutions(Instant retryDueAt, int batchSize, Instant now);

    int recoverStaleClaims(Instant leaseExpiredBefore, String recoveryWorkerId, Instant now);

    int markDeadExecutions(Instant deadline, String reason, Instant now);

    record ClaimRequest(
            String workerId,
            Instant claimedAt,
            Instant leaseExpiresAt,
            String leaseToken
    ) {
    }
}
