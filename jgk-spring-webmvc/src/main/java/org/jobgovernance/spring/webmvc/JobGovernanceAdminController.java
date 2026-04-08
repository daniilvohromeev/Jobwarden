package org.jobgovernance.spring.webmvc;

import org.jobgovernance.spring.core.JobGovernanceManagementService;
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

    public JobGovernanceAdminController(JobGovernanceManagementService managementService) {
        this.managementService = managementService;
    }

    @GetMapping("/jobs")
    public List<JobGovernanceManagementService.JobView> listJobs(
            @RequestParam(required = false) String tenantId,
            @RequestParam(defaultValue = "100") int limit
    ) {
        return managementService.listJobs(tenantId, limit);
    }

    @GetMapping("/jobs/{jobKey}/executions")
    public List<JobGovernanceManagementService.ExecutionView> listExecutions(
            @PathVariable String jobKey,
            @RequestParam(required = false) String tenantId,
            @RequestParam(defaultValue = "100") int limit
    ) {
        return managementService.listExecutions(jobKey, tenantId, limit);
    }

    @PostMapping("/jobs/{jobKey}/trigger")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public JobGovernanceManagementService.ExecutionView triggerNow(
            @PathVariable String jobKey,
            @RequestBody TriggerRequest request
    ) {
        return managementService.triggerNow(jobKey, request.tenantId(), request.actor(), request.payloadJson(), request.idempotencyKey());
    }

    @PostMapping("/jobs/{jobKey}/trigger-at")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public JobGovernanceManagementService.ExecutionView triggerAt(
            @PathVariable String jobKey,
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
    public void pauseJob(@PathVariable String jobKey, @RequestParam String actor) {
        managementService.pauseJob(jobKey, actor);
    }

    @PostMapping("/jobs/{jobKey}/resume")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void resumeJob(@PathVariable String jobKey, @RequestParam String actor) {
        managementService.resumeJob(jobKey, actor);
    }

    @PostMapping("/executions/{executionId}/cancel")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void cancelExecution(@PathVariable UUID executionId, @RequestBody CancelRequest request) {
        managementService.cancelExecution(executionId, request.actor(), request.reason());
    }

    public record TriggerRequest(String tenantId, String actor, String payloadJson, String idempotencyKey) {
    }

    public record TriggerAtRequest(String tenantId, String actor, String payloadJson, Instant triggerAt, String idempotencyKey) {
    }

    public record CancelRequest(String actor, String reason) {
    }
}
