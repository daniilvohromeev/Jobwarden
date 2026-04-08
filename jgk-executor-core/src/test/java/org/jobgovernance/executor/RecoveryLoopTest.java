package org.jobgovernance.executor;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RecoveryLoopTest {

    @Test
    void shouldRecoverStaleClaimsAndMarkDeadExecutions() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);
        FakeRepository repository = new FakeRepository();
        repository.recovered = 3;
        repository.dead = 2;
        RecoveryLoop loop = new RecoveryLoop(
                repository,
                "worker-recovery",
                Duration.ofSeconds(30),
                "stale execution deadline",
                clock
        );

        RecoveryLoop.RecoveryOutcome outcome = loop.recoverOnce(now);

        assertEquals(3, outcome.recoveredCount());
        assertEquals(2, outcome.markedDeadCount());
        assertEquals(now, repository.leaseExpiredBefore);
        assertEquals("worker-recovery", repository.recoveryWorkerId);
        assertEquals(now, repository.recoveryNow);
        assertEquals(now, repository.deadline);
        assertEquals("stale execution deadline", repository.deadReason);
        assertEquals(now, repository.deadNow);
    }

    private static final class FakeRepository extends ExecutionRepositoryStub {
        private Instant leaseExpiredBefore;
        private String recoveryWorkerId;
        private Instant recoveryNow;
        private Instant deadline;
        private String deadReason;
        private Instant deadNow;
        private int recovered;
        private int dead;

        @Override
        public int recoverStaleClaims(Instant leaseExpiredBefore, String recoveryWorkerId, Instant now) {
            this.leaseExpiredBefore = leaseExpiredBefore;
            this.recoveryWorkerId = recoveryWorkerId;
            this.recoveryNow = now;
            return recovered;
        }

        @Override
        public int markDeadExecutions(Instant deadline, String reason, Instant now) {
            this.deadline = deadline;
            this.deadReason = reason;
            this.deadNow = now;
            return dead;
        }
    }
}
