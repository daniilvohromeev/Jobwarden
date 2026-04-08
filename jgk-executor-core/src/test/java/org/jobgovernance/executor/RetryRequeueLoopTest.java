package org.jobgovernance.executor;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RetryRequeueLoopTest {

    @Test
    void shouldDelegateRetryRequeueToRepository() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);
        FakeRepository repository = new FakeRepository();
        repository.requeueResult = 7;
        RetryRequeueLoop loop = new RetryRequeueLoop(repository, Duration.ofSeconds(10), 42, clock);

        int requeued = loop.requeueDueRetriesOnce(now);

        assertEquals(7, requeued);
        assertEquals(now, repository.retryDueAt);
        assertEquals(42, repository.batchSize);
        assertEquals(now, repository.now);
    }

    private static final class FakeRepository extends ExecutionRepositoryStub {
        private Instant retryDueAt;
        private int batchSize;
        private Instant now;
        private int requeueResult;

        @Override
        public int requeueRetryableExecutions(Instant retryDueAt, int batchSize, Instant now) {
            this.retryDueAt = retryDueAt;
            this.batchSize = batchSize;
            this.now = now;
            return requeueResult;
        }
    }
}
