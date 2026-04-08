package org.jobgovernance.executor;

import org.jobgovernance.core.model.ExecutionStatus;
import org.jobgovernance.core.model.JobExecution;
import org.jobgovernance.core.model.TriggerType;
import org.jobgovernance.storage.spi.ExecutionRepository;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RepositoryClaimServiceTest {

    @Test
    void shouldClaimOnlyRowsWonByCompareAndSet() {
        FakeExecutionRepository repository = new FakeExecutionRepository();
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        UUID wonExecutionId = UUID.randomUUID();
        UUID lostExecutionId = UUID.randomUUID();
        repository.dueExecutionIds = List.of(wonExecutionId, lostExecutionId);
        repository.claimResponses.put(wonExecutionId, Optional.of(execution(wonExecutionId, ExecutionStatus.CLAIMED)));
        repository.claimResponses.put(lostExecutionId, Optional.empty());

        RepositoryClaimService claimService = new RepositoryClaimService(repository, Duration.ofSeconds(30));
        List<JobExecution> claimed = claimService.claimDueExecutions("worker-a", 10, now);

        assertEquals(1, claimed.size());
        assertEquals(wonExecutionId, claimed.get(0).executionId());
        assertEquals(2, repository.claimRequests.size());
        ExecutionRepository.ClaimRequest request = repository.claimRequests.get(wonExecutionId);
        assertEquals("worker-a", request.workerId());
        assertEquals(now, request.claimedAt());
        assertEquals(now.plusSeconds(30), request.leaseExpiresAt());
        assertNotNull(request.leaseToken());
    }

    @Test
    void shouldDelegateCancellationToRepository() {
        FakeExecutionRepository repository = new FakeExecutionRepository();
        repository.cancellationResult = true;
        RepositoryClaimService claimService = new RepositoryClaimService(repository, Duration.ofSeconds(5));
        UUID executionId = UUID.randomUUID();
        Instant now = Instant.parse("2026-01-01T00:00:00Z");

        boolean cancelled = claimService.requestCancel(executionId, "operator", "manual", now);

        assertTrue(cancelled);
        assertEquals(executionId, repository.cancelExecutionId);
        assertEquals("operator", repository.cancelActor);
        assertEquals("manual", repository.cancelReason);
        assertEquals(now, repository.cancelAt);
    }

    @Test
    void shouldValidateInputs() {
        FakeExecutionRepository repository = new FakeExecutionRepository();
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        RepositoryClaimService claimService = new RepositoryClaimService(repository, Duration.ofSeconds(5));

        assertThrows(IllegalArgumentException.class, () -> new RepositoryClaimService(null, Duration.ofSeconds(5)));
        assertThrows(IllegalArgumentException.class, () -> new RepositoryClaimService(repository, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> claimService.claimDueExecutions(" ", 10, now));
        assertThrows(IllegalArgumentException.class, () -> claimService.claimDueExecutions("worker", 0, now));
        assertThrows(IllegalArgumentException.class, () -> claimService.claimDueExecutions("worker", 1, null));
    }

    private static JobExecution execution(UUID executionId, ExecutionStatus status) {
        return new JobExecution(
                executionId,
                "job-a",
                TriggerType.CRON,
                Instant.parse("2026-01-01T00:00:00Z"),
                null,
                null,
                null,
                status,
                null,
                1,
                null,
                null,
                null,
                false,
                0L,
                null,
                null,
                null,
                null,
                null,
                null
        );
    }

    private static final class FakeExecutionRepository implements ExecutionRepository {

        List<UUID> dueExecutionIds = List.of();
        Map<UUID, Optional<JobExecution>> claimResponses = new HashMap<>();
        Map<UUID, ClaimRequest> claimRequests = new HashMap<>();
        boolean cancellationResult;
        UUID cancelExecutionId;
        String cancelActor;
        String cancelReason;
        Instant cancelAt;

        @Override
        public List<UUID> findDueExecutionIds(Instant now, int batchSize) {
            return new ArrayList<>(dueExecutionIds);
        }

        @Override
        public Optional<JobExecution> claimExecution(UUID executionId, ClaimRequest request) {
            claimRequests.put(executionId, request);
            return claimResponses.getOrDefault(executionId, Optional.empty());
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
        public boolean markFailedRetryable(UUID executionId, String workerId, String leaseToken, String errorClass, String errorSummary, Instant nextRetryAt, Instant finishedAt) {
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
            this.cancelExecutionId = executionId;
            this.cancelActor = actor;
            this.cancelReason = reason;
            this.cancelAt = requestedAt;
            return cancellationResult;
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
}
