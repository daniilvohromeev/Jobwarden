package org.jobgovernance.executor;

import org.jobgovernance.core.api.ClaimService;
import org.jobgovernance.core.model.JobExecution;
import org.jobgovernance.storage.spi.ExecutionRepository;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public final class RepositoryClaimService implements ClaimService {

    private final ExecutionRepository executionRepository;
    private final Duration leaseTtl;

    public RepositoryClaimService(ExecutionRepository executionRepository, Duration leaseTtl) {
        if (executionRepository == null) {
            throw new IllegalArgumentException("executionRepository is required");
        }
        if (leaseTtl == null || leaseTtl.isNegative() || leaseTtl.isZero()) {
            throw new IllegalArgumentException("leaseTtl must be positive");
        }
        this.executionRepository = executionRepository;
        this.leaseTtl = leaseTtl;
    }

    @Override
    public List<JobExecution> claimDueExecutions(String workerId, int batchSize, Instant now) {
        if (workerId == null || workerId.isBlank()) {
            throw new IllegalArgumentException("workerId is required");
        }
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }
        if (now == null) {
            throw new IllegalArgumentException("now is required");
        }

        List<UUID> dueExecutionIds = executionRepository.findDueExecutionIds(now, batchSize);
        List<JobExecution> claimed = new ArrayList<>(dueExecutionIds.size());

        for (UUID executionId : dueExecutionIds) {
            ExecutionRepository.ClaimRequest claimRequest = new ExecutionRepository.ClaimRequest(
                    workerId,
                    now,
                    now.plus(leaseTtl),
                    UUID.randomUUID().toString()
            );
            executionRepository.claimExecution(executionId, claimRequest).ifPresent(claimed::add);
        }
        return claimed;
    }

    @Override
    public boolean requestCancel(UUID executionId, String actor, String reason, Instant now) {
        return executionRepository.requestCancellation(executionId, actor, reason, now);
    }
}
