package org.jobgovernance.executor;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CleanupLoopTest {

    @Test
    void cleanupOnceShouldDeleteRowsBeforeRetentionCutoff() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        FakeExecutionRepository repository = new FakeExecutionRepository();
        repository.cleanupResult = 7;
        CleanupLoop loop = new CleanupLoop(
                repository,
                Duration.ofSeconds(5),
                Duration.ofHours(24),
                500,
                Clock.fixed(now, ZoneId.of("UTC"))
        );

        int cleaned = loop.cleanupOnce(now);

        assertEquals(7, cleaned);
        assertEquals(now.minus(Duration.ofHours(24)), repository.lastCleanupCutoff);
        assertEquals(500, repository.lastCleanupBatchSize);
        assertEquals(now, repository.lastCleanupNow);
    }

    @Test
    void constructorShouldRejectInvalidArguments() {
        FakeExecutionRepository repository = new FakeExecutionRepository();

        assertThrows(
                IllegalArgumentException.class,
                () -> new CleanupLoop(repository, Duration.ZERO, Duration.ofHours(1), 100)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> new CleanupLoop(repository, Duration.ofSeconds(1), Duration.ZERO, 100)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> new CleanupLoop(repository, Duration.ofSeconds(1), Duration.ofHours(1), 0)
        );
    }

    private static final class FakeExecutionRepository extends ExecutionRepositoryStub {
        int cleanupResult;
        Instant lastCleanupCutoff;
        int lastCleanupBatchSize;
        Instant lastCleanupNow;

        @Override
        public int cleanupFinishedExecutions(Instant finishedBefore, int batchSize, Instant now) {
            this.lastCleanupCutoff = finishedBefore;
            this.lastCleanupBatchSize = batchSize;
            this.lastCleanupNow = now;
            return cleanupResult;
        }
    }
}
