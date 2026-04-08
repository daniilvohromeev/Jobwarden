package org.jobgovernance.executor;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HeartbeatLoopTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @Test
    void shouldRenewLeasesForTrackedExecutions() {
        InMemoryActiveExecutionTracker tracker = new InMemoryActiveExecutionTracker();
        UUID firstExecution = UUID.randomUUID();
        UUID secondExecution = UUID.randomUUID();
        tracker.add(new ActiveExecutionTracker.ActiveExecution(firstExecution, "worker-a", "lease-a", Duration.ofSeconds(30)));
        tracker.add(new ActiveExecutionTracker.ActiveExecution(secondExecution, "worker-a", "lease-b", Duration.ofSeconds(45)));
        FakeRepository repository = new FakeRepository();
        HeartbeatLoop loop = new HeartbeatLoop(repository, tracker, Duration.ofSeconds(5), CLOCK);

        int renewed = loop.renewLeasesOnce(NOW);

        assertEquals(2, renewed);
        assertEquals(2, repository.renewCalls.size());
        assertTrue(
                repository.renewCalls.stream().anyMatch(call ->
                        call.executionId().equals(firstExecution)
                                && call.leaseExpiresAt().equals(NOW.plusSeconds(30))
                )
        );
        assertTrue(
                repository.renewCalls.stream().anyMatch(call ->
                        call.executionId().equals(secondExecution)
                                && call.leaseExpiresAt().equals(NOW.plusSeconds(45))
                )
        );
    }

    @Test
    void shouldDropExecutionFromTrackerWhenLeaseRenewalIsRejected() {
        InMemoryActiveExecutionTracker tracker = new InMemoryActiveExecutionTracker();
        UUID executionId = UUID.randomUUID();
        tracker.add(new ActiveExecutionTracker.ActiveExecution(executionId, "worker-a", "lease-a", Duration.ofSeconds(30)));
        FakeRepository repository = new FakeRepository();
        repository.rejectedExecution = executionId;
        HeartbeatLoop loop = new HeartbeatLoop(repository, tracker, Duration.ofSeconds(5), CLOCK);

        int renewed = loop.renewLeasesOnce(NOW);

        assertEquals(0, renewed);
        assertEquals(0, tracker.snapshot().size());
    }

    private static final class FakeRepository extends ExecutionRepositoryStub {
        private final List<RenewCall> renewCalls = new ArrayList<>();
        private UUID rejectedExecution;

        @Override
        public boolean renewLease(UUID executionId, String workerId, String leaseToken, Instant leaseExpiresAt, Instant heartbeatAt) {
            renewCalls.add(new RenewCall(executionId, workerId, leaseToken, leaseExpiresAt, heartbeatAt));
            return !executionId.equals(rejectedExecution);
        }
    }

    private record RenewCall(
            UUID executionId,
            String workerId,
            String leaseToken,
            Instant leaseExpiresAt,
            Instant heartbeatAt
    ) {
    }
}
