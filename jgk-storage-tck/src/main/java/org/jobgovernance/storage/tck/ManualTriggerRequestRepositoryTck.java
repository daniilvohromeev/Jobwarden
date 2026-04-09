package org.jobgovernance.storage.tck;

import org.jobgovernance.storage.spi.ManualTriggerRequestRepository;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public interface ManualTriggerRequestRepositoryTck extends StorageContractSupport {

    ManualTriggerRequestRepository manualTriggerRequestRepository();

    @Test
    default void shouldEnqueueAndFetchPendingRequests() {
        clearStorage();
        String jobKey = "tck.trigger.fetch";
        Instant now = fixedNow();
        seedDefinition(jobKey);

        UUID requestId = manualTriggerRequestRepository().enqueue(
                new ManualTriggerRequestRepository.ManualTriggerRequest(
                        UUID.randomUUID(),
                        jobKey,
                        null,
                        "inline-json:{\"x\":1}",
                        "idem-fetch",
                        now.minusSeconds(1),
                        "tck",
                        now.minusSeconds(2)
                )
        );

        var pending = manualTriggerRequestRepository().fetchPending(10, now);
        assertEquals(1, pending.size());
        assertEquals(requestId, pending.getFirst().requestId());
    }

    @Test
    default void shouldDeduplicateByIdempotencyKey() {
        clearStorage();
        String jobKey = "tck.trigger.idem";
        Instant now = fixedNow();
        seedDefinition(jobKey);

        UUID first = manualTriggerRequestRepository().enqueue(
                new ManualTriggerRequestRepository.ManualTriggerRequest(
                        UUID.randomUUID(),
                        jobKey,
                        null,
                        null,
                        "idem-dup",
                        now,
                        "tck",
                        now
                )
        );
        UUID second = manualTriggerRequestRepository().enqueue(
                new ManualTriggerRequestRepository.ManualTriggerRequest(
                        UUID.randomUUID(),
                        jobKey,
                        null,
                        null,
                        "idem-dup",
                        now,
                        "tck",
                        now.plusSeconds(1)
                )
        );

        assertEquals(first, second);
    }

    @Test
    default void shouldMarkPendingRequestAsProcessedOnce() {
        clearStorage();
        String jobKey = "tck.trigger.processed";
        Instant now = fixedNow();
        seedDefinition(jobKey);
        UUID executionId = seedScheduledExecution(jobKey, now.minusSeconds(5));
        UUID requestId = manualTriggerRequestRepository().enqueue(
                new ManualTriggerRequestRepository.ManualTriggerRequest(
                        UUID.randomUUID(),
                        jobKey,
                        null,
                        "inline-json:{}",
                        "idem-process",
                        now.minusSeconds(1),
                        "tck",
                        now.minusSeconds(1)
                )
        );

        boolean processed = manualTriggerRequestRepository().markProcessed(requestId, now, executionId);
        boolean processedAgain = manualTriggerRequestRepository().markProcessed(requestId, now.plusSeconds(1), executionId);

        assertTrue(processed);
        assertFalse(processedAgain);
        assertTrue(manualTriggerRequestRepository().fetchPending(10, now.plusSeconds(2)).isEmpty());
    }
}
