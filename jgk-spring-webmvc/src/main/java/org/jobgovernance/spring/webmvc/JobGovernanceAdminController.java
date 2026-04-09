package org.jobgovernance.spring.webmvc;

import org.jobgovernance.spring.core.JobGovernanceManagementService;
import org.jobgovernance.spring.core.JobGovernanceRuntimeStatusService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/jgk/v1")
public class JobGovernanceAdminController {

    private final JobGovernanceManagementService managementService;
    private final JobGovernanceRuntimeStatusService runtimeStatusService;

    public JobGovernanceAdminController(JobGovernanceManagementService managementService) {
        this(managementService, null);
    }

    public JobGovernanceAdminController(
            JobGovernanceManagementService managementService,
            JobGovernanceRuntimeStatusService runtimeStatusService
    ) {
        this.managementService = managementService;
        this.runtimeStatusService = runtimeStatusService;
    }

    @GetMapping("/jobs")
    public List<JobGovernanceManagementService.JobView> listJobs(
            @RequestParam(name = "tenantId", required = false) String tenantId,
            @RequestParam(name = "limit", defaultValue = "100") int limit
    ) {
        return managementService.listJobs(tenantId, limit);
    }

    @GetMapping("/health")
    public JobGovernanceRuntimeStatusService.RuntimeStatus health() {
        return runtimeStatusService == null
                ? new JobGovernanceRuntimeStatusService.RuntimeStatus("UNKNOWN", "unknown-worker", false, 0, 0, 0, Instant.now())
                : runtimeStatusService.health();
    }

    @GetMapping("/readiness")
    public JobGovernanceRuntimeStatusService.RuntimeStatus readiness() {
        return runtimeStatusService == null
                ? new JobGovernanceRuntimeStatusService.RuntimeStatus("UNKNOWN", "unknown-worker", false, 0, 0, 0, Instant.now())
                : runtimeStatusService.readiness();
    }

    @GetMapping("/jobs/{jobKey}/executions")
    public List<JobGovernanceManagementService.ExecutionView> listExecutions(
            @PathVariable("jobKey") String jobKey,
            @RequestParam(name = "tenantId", required = false) String tenantId,
            @RequestParam(name = "limit", defaultValue = "100") int limit
    ) {
        return managementService.listExecutions(jobKey, tenantId, limit);
    }

    @PostMapping("/jobs/{jobKey}/trigger")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public JobGovernanceManagementService.ExecutionView triggerNow(
            @PathVariable("jobKey") String jobKey,
            @RequestBody TriggerRequest request
    ) {
        return managementService.triggerNow(jobKey, request.tenantId(), request.actor(), request.payloadJson(), request.idempotencyKey());
    }

    @PostMapping("/jobs/{jobKey}/trigger-at")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public JobGovernanceManagementService.ExecutionView triggerAt(
            @PathVariable("jobKey") String jobKey,
            @RequestBody TriggerAtRequest request
    ) {
        return managementService.triggerAt(
                jobKey,
                request.tenantId(),
                request.actor(),
                request.payloadJson(),
                request.triggerAt(),
                request.idempotencyKey()
        );
    }

    @PostMapping("/jobs/{jobKey}/pause")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void pauseJob(
            @PathVariable("jobKey") String jobKey,
            @RequestParam(name = "actor") String actor
    ) {
        managementService.pauseJob(jobKey, actor);
    }

    @PostMapping("/jobs/{jobKey}/resume")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void resumeJob(
            @PathVariable("jobKey") String jobKey,
            @RequestParam(name = "actor") String actor
    ) {
        managementService.resumeJob(jobKey, actor);
    }

    @PostMapping("/executions/{executionId}/cancel")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void cancelExecution(
            @PathVariable("executionId") UUID executionId,
            @RequestBody CancelRequest request
    ) {
        managementService.cancelExecution(executionId, request.actor(), request.reason());
    }

    public record TriggerRequest(String tenantId, String actor, String payloadJson, String idempotencyKey) {
    }

    public record TriggerAtRequest(String tenantId, String actor, String payloadJson, Instant triggerAt, String idempotencyKey) {
    }

    public record CancelRequest(String actor, String reason) {
    }
}
