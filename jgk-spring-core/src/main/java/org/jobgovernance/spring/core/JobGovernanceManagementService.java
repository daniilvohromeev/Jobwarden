package org.jobgovernance.spring.core;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public interface JobGovernanceManagementService {

    List<JobView> listJobs(String tenantId, int limit);

    ExecutionView triggerNow(String jobKey, String tenantId, String actor, String payloadJson, String idempotencyKey);

    ExecutionView triggerAt(String jobKey, String tenantId, String actor, String payloadJson, Instant triggerAt, String idempotencyKey);

    ExecutionView retryExecution(UUID executionId, String actor);

    boolean pauseJob(String jobKey, String actor);

    boolean resumeJob(String jobKey, String actor);

    boolean cancelExecution(UUID executionId, String actor, String reason);

    List<ExecutionView> listExecutions(String jobKey, String tenantId, int limit);

    List<AuditView> listAuditEventsByJob(String jobKey, int limit);

    List<AuditView> listAuditEventsByExecution(UUID executionId, int limit);

    default List<WorkerView> listActiveWorkers(int limit) {
        return List.of();
    }

    default Optional<JobView> getJob(String jobKey, String tenantId) {
        if (jobKey == null || jobKey.isBlank()) {
            return Optional.empty();
        }
        return listJobs(tenantId, 1_000).stream()
                .filter(job -> job.jobKey().equals(jobKey))
                .findFirst();
    }

    default List<ExecutionView> listRetryExecutions(String tenantId, int limit) {
        return listExecutionsByStatus(tenantId, limit, "FAILED_RETRYABLE");
    }

    default List<ExecutionView> listDeadExecutions(String tenantId, int limit) {
        return listExecutionsByStatus(tenantId, limit, "DEAD");
    }

    private List<ExecutionView> listExecutionsByStatus(String tenantId, int limit, String status) {
        int cappedLimit = limit <= 0 ? 100 : Math.min(limit, 1_000);
        List<ExecutionView> collected = new ArrayList<>(cappedLimit);
        for (JobView job : listJobs(tenantId, cappedLimit)) {
            if (collected.size() >= cappedLimit) {
                break;
            }
            List<ExecutionView> executions = listExecutions(job.jobKey(), tenantId, cappedLimit);
            for (ExecutionView execution : executions) {
                if (status.equals(execution.status())) {
                    collected.add(execution);
                    if (collected.size() >= cappedLimit) {
                        break;
                    }
                }
            }
        }
        collected.sort(Comparator.comparing(
                ExecutionView::scheduledAt,
                Comparator.nullsLast(Comparator.reverseOrder())
        ));
        return collected;
    }

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

    record AuditView(
            UUID eventId,
            String eventType,
            String jobKey,
            UUID executionId,
            String actor,
            String detailsJson,
            Instant createdAt
    ) {
    }

    record WorkerView(
            String workerId,
            int activeExecutions,
            Instant oldestClaimedAt,
            Instant lastHeartbeatAt,
            Instant leaseExpiresAt
    ) {
    }
}
