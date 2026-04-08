package org.jobgovernance.storage.postgres;

import org.jobgovernance.core.model.JobExecution;
import org.jobgovernance.storage.spi.ExecutionRepository;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class PostgresExecutionRepository implements ExecutionRepository {

    private final DataSource dataSource;

    public PostgresExecutionRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public List<UUID> findDueExecutionIds(Instant now, int batchSize) {
        throw new UnsupportedOperationException("SQL implementation pending");
    }

    @Override
    public Optional<JobExecution> claimExecution(UUID executionId, ClaimRequest request) {
        throw new UnsupportedOperationException("SQL implementation pending");
    }

    @Override
    public boolean markRunning(UUID executionId, String workerId, String leaseToken, Instant startedAt) {
        throw new UnsupportedOperationException("SQL implementation pending");
    }

    @Override
    public boolean markSucceeded(UUID executionId, String workerId, String leaseToken, String resultSummary, Instant finishedAt) {
        throw new UnsupportedOperationException("SQL implementation pending");
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
        throw new UnsupportedOperationException("SQL implementation pending");
    }

    @Override
    public boolean markFailedFinal(UUID executionId, String workerId, String leaseToken, String errorClass, String errorSummary, Instant finishedAt) {
        throw new UnsupportedOperationException("SQL implementation pending");
    }

    @Override
    public boolean markTimedOut(UUID executionId, String workerId, String leaseToken, String reason, Instant finishedAt) {
        throw new UnsupportedOperationException("SQL implementation pending");
    }

    @Override
    public boolean requestCancellation(UUID executionId, String actor, String reason, Instant requestedAt) {
        throw new UnsupportedOperationException("SQL implementation pending");
    }

    @Override
    public boolean markCancelled(UUID executionId, String workerId, String leaseToken, String reason, Instant finishedAt) {
        throw new UnsupportedOperationException("SQL implementation pending");
    }

    @Override
    public boolean renewLease(UUID executionId, String workerId, String leaseToken, Instant leaseExpiresAt, Instant heartbeatAt) {
        throw new UnsupportedOperationException("SQL implementation pending");
    }

    @Override
    public int recoverStaleClaims(Instant leaseExpiredBefore, String recoveryWorkerId, Instant now) {
        throw new UnsupportedOperationException("SQL implementation pending");
    }

    @Override
    public int markDeadExecutions(Instant deadline, String reason, Instant now) {
        throw new UnsupportedOperationException("SQL implementation pending");
    }
}
