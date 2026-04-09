package org.jobgovernance.storage.tck;

import org.jobgovernance.storage.spi.AuditEventRepository;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public interface AuditEventRepositoryTck extends StorageContractSupport {

    AuditEventRepository auditEventRepository();

    @Test
    default void shouldAppendAndFindByJobKey() {
        clearStorage();
        String jobKey = "tck.audit.job";
        var now = fixedNow();
        seedDefinition(jobKey);

        auditEventRepository().append(new AuditEventRepository.AuditEvent(
                UUID.randomUUID(),
                "JOB_PAUSED",
                jobKey,
                null,
                "tck",
                "{\"reason\":\"contract\"}",
                now
        ));
        auditEventRepository().append(new AuditEventRepository.AuditEvent(
                UUID.randomUUID(),
                "JOB_RESUMED",
                jobKey,
                null,
                "tck",
                "{\"reason\":\"contract\"}",
                now.plusSeconds(1)
        ));

        List<AuditEventRepository.AuditEvent> byJob = auditEventRepository().findByJobKey(jobKey, 10);
        List<AuditEventRepository.AuditEvent> limited = auditEventRepository().findByJobKey(jobKey, 1);
        assertEquals(2, byJob.size());
        assertEquals("JOB_RESUMED", byJob.getFirst().eventType());
        assertEquals(1, limited.size());
        assertEquals("JOB_RESUMED", limited.getFirst().eventType());
    }

    @Test
    default void shouldFindByExecutionId() {
        clearStorage();
        String jobKey = "tck.audit.execution";
        var now = fixedNow();
        seedDefinition(jobKey);
        UUID executionId = seedScheduledExecution(jobKey, now.minusSeconds(1));

        auditEventRepository().append(new AuditEventRepository.AuditEvent(
                UUID.randomUUID(),
                "EXECUTION_CANCELLED",
                jobKey,
                executionId,
                "tck",
                "{\"reason\":\"contract\"}",
                now
        ));

        List<AuditEventRepository.AuditEvent> byExecution = auditEventRepository().findByExecutionId(executionId, 10);
        assertEquals(1, byExecution.size());
        assertEquals(executionId, byExecution.getFirst().executionId());
        assertTrue(byExecution.getFirst().detailsJson().contains("contract"));
    }
}
