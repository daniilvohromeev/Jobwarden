package org.jobgovernance.storage.tck;

import org.jobgovernance.core.model.JobDefinitionState;
import org.jobgovernance.storage.spi.JobDefinitionRepository;
import org.jobgovernance.storage.spi.ScheduleCursorRepository;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public interface ScheduleCursorRepositoryTck extends StorageContractSupport {

    ScheduleCursorRepository scheduleCursorRepository();

    JobDefinitionRepository jobDefinitionRepository();

    @Test
    default void shouldReturnEmptyForMissingCursor() {
        clearStorage();
        assertTrue(scheduleCursorRepository().findByJobKey("missing.job").isEmpty());
    }

    @Test
    default void shouldUpdateCursorWithVersionCheck() {
        clearStorage();
        String jobKey = "tck.cursor.versioned";
        var now = fixedNow();
        jobDefinitionRepository().upsert(sampleDefinition(jobKey, JobDefinitionState.ENABLED), now);

        var current = scheduleCursorRepository().findByJobKey(jobKey).orElseThrow();
        scheduleCursorRepository().updateCursor(
                new ScheduleCursorRepository.ScheduleCursor(
                        jobKey,
                        now,
                        now.plusSeconds(90),
                        current.cursorVersion()
                ),
                now.plusSeconds(1)
        );

        var updated = scheduleCursorRepository().findByJobKey(jobKey).orElseThrow();
        assertEquals(current.cursorVersion() + 1, updated.cursorVersion());
        assertEquals(now, updated.lastEvaluatedAt());
        assertEquals(now.plusSeconds(90), updated.nextMaterializeAt());
    }
}
