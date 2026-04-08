package org.jobgovernance.storage.spi;

import java.time.Instant;
import java.util.Optional;

public interface ScheduleCursorRepository {

    Optional<ScheduleCursor> findByJobKey(String jobKey);

    void updateCursor(ScheduleCursor cursor, Instant updatedAt);

    record ScheduleCursor(
            String jobKey,
            Instant lastEvaluatedAt,
            Instant nextMaterializeAt,
            long cursorVersion
    ) {
    }
}
