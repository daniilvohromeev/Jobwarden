package org.jobgovernance.spring.core;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface JobGovernanceManagementService {

    List<JobView> listJobs(String tenantId, int limit);

    ExecutionView triggerNow(String jobKey, String tenantId, String actor, String payloadJson, String idempotencyKey);

    ExecutionView triggerAt(String jobKey, String tenantId, String actor, String payloadJson, Instant triggerAt, String idempotencyKey);

    boolean pauseJob(String jobKey, String actor);

    boolean resumeJob(String jobKey, String actor);

    boolean cancelExecution(UUID executionId, String actor, String reason);

    List<ExecutionView> listExecutions(String jobKey, String tenantId, int limit);

    record JobView(
            String jobKey,
            String displayName,
            String state,
            String ownerTeam,
            List<String> tags
    ) {
    }

    record ExecutionView(
            UUID executionId,
            String jobKey,
            String status,
            Instant scheduledAt,
            Instant startedAt,
            Instant finishedAt,
            int attempt,
            String workerId,
            Map<String, String> links
    ) {
    }
}
