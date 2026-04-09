package org.jobgovernance.storage.tck;

import org.jobgovernance.core.model.ExecutionStatus;
import org.jobgovernance.storage.spi.ExecutionRepository;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public interface ExecutionRepositoryTck extends StorageContractSupport {

    ExecutionRepository executionRepository();

    @Test
    default void shouldEnqueueDueAndClaimSingleExecution() {
        clearStorage();
        seedDefinition("tck.execution.claim");
        Instant now = fixedNow();

        boolean inserted = executionRepository().enqueueScheduledExecution(
                new ExecutionRepository.ScheduledExecutionInsert(
                        "tck.execution.claim",
                        null,
                        "MANUAL",
                        now.plusSeconds(2),
                        now.plusSeconds(2),
                        3,
                        "inline-json:{}",
                        "dedupe-claim",
                        "corr-claim",
                        null,
                        null,
                        null,
                        "idem-claim",
                        "business-claim"
                ),
                now
        );
        assertTrue(inserted);

        List<UUID> due = executionRepository().findDueExecutionIds(now.plusSeconds(3), 10);
        assertEquals(1, due.size());

        UUID executionId = due.getFirst();
        var firstClaim = executionRepository().claimExecution(
                executionId,
                new ExecutionRepository.ClaimRequest("worker-1", now.plusSeconds(3), now.plusSeconds(33), "lease-1")
        );
        var secondClaim = executionRepository().claimExecution(
                executionId,
                new ExecutionRepository.ClaimRequest("worker-2", now.plusSeconds(4), now.plusSeconds(34), "lease-2")
        );

        assertTrue(firstClaim.isPresent());
        assertFalse(secondClaim.isPresent());
        assertEquals(ExecutionStatus.CLAIMED, firstClaim.get().status());
    }

    @Test
    default void shouldLookupByIdempotencyKey() {
        clearStorage();
        seedDefinition("tck.execution.idempotency");
        Instant now = fixedNow();
        String idempotencyKey = "idem-" + now.toEpochMilli();

        executionRepository().enqueueScheduledExecution(
                new ExecutionRepository.ScheduledExecutionInsert(
                        "tck.execution.idempotency",
                        null,
                        "MANUAL",
                        now,
                        now,
                        2,
                        null,
                        "dedupe-idem",
                        null,
                        null,
                        null,
                        null,
                        idempotencyKey,
                        null
                ),
                now
        );

        var found = executionRepository().findByIdempotencyKey("tck.execution.idempotency", null, idempotencyKey);
        assertTrue(found.isPresent());
        assertEquals("tck.execution.idempotency", found.get().jobKey());
        assertEquals(ExecutionStatus.SCHEDULED, found.get().status());
    }

    @Test
    default void shouldGuardRunningAndSuccessByLeaseOwner() {
        clearStorage();
        seedDefinition("tck.execution.lease-guard");
        Instant now = fixedNow();

        UUID executionId = seedScheduledExecution("tck.execution.lease-guard", now.minusSeconds(1));
        var claim = executionRepository().claimExecution(
                executionId,
                new ExecutionRepository.ClaimRequest("worker-lease", now, now.plusSeconds(45), "lease-guard")
        );
        assertTrue(claim.isPresent());

        boolean wrongLeaseRun = executionRepository().markRunning(executionId, "worker-lease", "wrong-lease", now.plusSeconds(1));
        boolean running = executionRepository().markRunning(executionId, "worker-lease", "lease-guard", now.plusSeconds(1));
        boolean wrongLeaseSuccess = executionRepository().markSucceeded(executionId, "worker-lease", "wrong-lease", "ok", now.plusSeconds(3));
        boolean success = executionRepository().markSucceeded(executionId, "worker-lease", "lease-guard", "ok", now.plusSeconds(3));

        assertFalse(wrongLeaseRun);
        assertTrue(running);
        assertFalse(wrongLeaseSuccess);
        assertTrue(success);

        var execution = executionRepository().findExecution(executionId).orElseThrow();
        assertEquals(ExecutionStatus.SUCCEEDED, execution.status());
    }

    @Test
    default void shouldCancelScheduledExecutionBeforeStart() {
        clearStorage();
        seedDefinition("tck.execution.cancel");
        Instant now = fixedNow();

        UUID executionId = seedScheduledExecution("tck.execution.cancel", now.plusSeconds(30));
        boolean cancelled = executionRepository().requestCancellation(
                executionId,
                "tck-actor",
                "cancel by contract test",
                now.plusSeconds(1)
        );

        assertTrue(cancelled);
        var execution = executionRepository().findExecution(executionId).orElseThrow();
        assertEquals(ExecutionStatus.CANCELLED, execution.status());
        assertTrue(execution.cancellationRequested());
    }

    @Test
    default void shouldRequeueRetryableExecutions() {
        clearStorage();
        seedDefinition("tck.execution.retry");
        Instant now = fixedNow();

        UUID executionId = seedScheduledExecution("tck.execution.retry", now.minusSeconds(2));
        var claim = executionRepository().claimExecution(
                executionId,
                new ExecutionRepository.ClaimRequest("worker-retry", now.minusSeconds(1), now.plusSeconds(30), "lease-retry")
        );
        assertTrue(claim.isPresent());
        assertTrue(executionRepository().markRunning(executionId, "worker-retry", "lease-retry", now));
        assertTrue(executionRepository().markFailedRetryable(
                executionId,
                "worker-retry",
                "lease-retry",
                "java.lang.RuntimeException",
                "retry me",
                now.plusSeconds(5),
                now.plusSeconds(1)
        ));

        int requeued = executionRepository().requeueRetryableExecutions(now.plusSeconds(5), 10, now.plusSeconds(6));
        assertEquals(1, requeued);

        var execution = executionRepository().findExecution(executionId).orElseThrow();
        assertEquals(ExecutionStatus.SCHEDULED, execution.status());
        assertEquals(2, execution.attempt());
        assertNotNull(execution.scheduledAt());
    }

    @Test
    default void shouldReportActiveWorkers() {
        clearStorage();
        seedDefinition("tck.execution.workers");
        Instant now = fixedNow();

        UUID a1 = seedScheduledExecution("tck.execution.workers", now.minusSeconds(2));
        UUID a2 = seedScheduledExecution("tck.execution.workers", now.minusSeconds(1));
        UUID b1 = seedScheduledExecution("tck.execution.workers", now);

        assertTrue(executionRepository().claimExecution(
                a1,
                new ExecutionRepository.ClaimRequest("worker-a", now, now.plusSeconds(60), "lease-a1")
        ).isPresent());
        assertTrue(executionRepository().claimExecution(
                a2,
                new ExecutionRepository.ClaimRequest("worker-a", now.plusSeconds(1), now.plusSeconds(60), "lease-a2")
        ).isPresent());
        assertTrue(executionRepository().markRunning(a2, "worker-a", "lease-a2", now.plusSeconds(2)));
        assertTrue(executionRepository().claimExecution(
                b1,
                new ExecutionRepository.ClaimRequest("worker-b", now.plusSeconds(3), now.plusSeconds(60), "lease-b1")
        ).isPresent());

        List<ExecutionRepository.WorkerStatus> workers = executionRepository().findActiveWorkers(now.plusSeconds(4), 10);

        assertEquals(2, workers.size());
        assertEquals("worker-a", workers.getFirst().workerId());
        assertEquals(2, workers.getFirst().activeExecutions());
        assertNotNull(workers.getFirst().leaseExpiresAt());
        assertEquals("worker-b", workers.get(1).workerId());
        assertEquals(1, workers.get(1).activeExecutions());
    }
}
