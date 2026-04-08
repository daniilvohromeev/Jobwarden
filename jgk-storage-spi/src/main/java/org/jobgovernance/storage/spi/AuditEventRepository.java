package org.jobgovernance.storage.spi;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface AuditEventRepository {

    void append(AuditEvent event);

    List<AuditEvent> findByJobKey(String jobKey, int limit);

    List<AuditEvent> findByExecutionId(UUID executionId, int limit);

    record AuditEvent(
            UUID eventId,
            String eventType,
            String jobKey,
            UUID executionId,
            String actor,
            String detailsJson,
            Instant createdAt
    ) {
    }
}
