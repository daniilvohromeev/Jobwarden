package org.jobgovernance.executor;

import org.jobgovernance.core.model.JobExecution;
import org.jobgovernance.storage.spi.ExecutionRepository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

abstract class ExecutionRepositoryStub implements ExecutionRepository {

    @Override
    public List<UUID> findDueExecutionIds(Instant now, int batchSize) {
        return List.of();
    }

    @Override
    public Optional<JobExecution> claimExecution(UUID executionId, ClaimRequest request) {
        return Optional.empty();
    }

    @Override
    public boolean enqueueScheduledExecution(ScheduledExecutionInsert request, Instant createdAt) {
        return false;
    }

    @Override
    public boolean markRunning(UUID executionId, String workerId, String leaseToken, Instant startedAt) {
        return false;
    }

    @Override
    public boolean markSucceeded(UUID executionId, String workerId, String leaseToken, String resultSummary, Instant finishedAt) {
        return false;
    }

    @Override
    public boolean markFailedRetryable(
            UUID executionId,
            String workerId,
            String leaseToken,
            String errorClass,
            String errorSummary,
            Instant nextRetryAt,
            Instant finishedAt
    ) {
        return false;
    }

    @Override
    public boolean markFailedFinal(UUID executionId, String workerId, String leaseToken, String errorClass, String errorSummary, Instant finishedAt) {
        return false;
    }

    @Override
    public boolean markTimedOut(UUID executionId, String workerId, String leaseToken, String reason, Instant finishedAt) {
        return false;
    }

    @Override
    public boolean requestCancellation(UUID executionId, String actor, String reason, Instant requestedAt) {
        return false;
    }

    @Override
    public boolean markCancelled(UUID executionId, String workerId, String leaseToken, String reason, Instant finishedAt) {
        return false;
    }

    @Override
    public boolean renewLease(UUID executionId, String workerId, String leaseToken, Instant leaseExpiresAt, Instant heartbeatAt) {
        return false;
    }

    @Override
    public int requeueRetryableExecutions(Instant retryDueAt, int batchSize, Instant now) {
        return 0;
    }

    @Override
    public int recoverStaleClaims(Instant leaseExpiredBefore, String recoveryWorkerId, Instant now) {
        return 0;
    }

    @Override
    public int markDeadExecutions(Instant deadline, String reason, Instant now) {
        return 0;
    }
}
