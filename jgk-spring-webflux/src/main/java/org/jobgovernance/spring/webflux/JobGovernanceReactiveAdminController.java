package org.jobgovernance.spring.webflux;

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
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/jgk/v1")
public class JobGovernanceReactiveAdminController {

    private final JobGovernanceManagementService managementService;
    private final JobGovernanceRuntimeStatusService runtimeStatusService;

    public JobGovernanceReactiveAdminController(JobGovernanceManagementService managementService) {
        this(managementService, null);
    }

    public JobGovernanceReactiveAdminController(
            JobGovernanceManagementService managementService,
            JobGovernanceRuntimeStatusService runtimeStatusService
    ) {
        this.managementService = managementService;
        this.runtimeStatusService = runtimeStatusService;
    }

    @GetMapping("/jobs")
    public Flux<JobGovernanceManagementService.JobView> listJobs(
            @RequestParam(name = "tenantId", required = false) String tenantId,
            @RequestParam(name = "limit", defaultValue = "100") int limit
    ) {
        return Flux.fromIterable(managementService.listJobs(tenantId, limit));
    }

    @GetMapping("/jobs/{jobKey}")
    public Mono<JobGovernanceManagementService.JobView> getJob(
            @PathVariable("jobKey") String jobKey,
            @RequestParam(name = "tenantId", required = false) String tenantId
    ) {
        return Mono.fromSupplier(() -> managementService.getJob(jobKey, tenantId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "jobKey not found: " + jobKey)));
    }

    @GetMapping("/health")
    public Mono<JobGovernanceRuntimeStatusService.RuntimeStatus> health() {
        if (runtimeStatusService == null) {
            return Mono.just(new JobGovernanceRuntimeStatusService.RuntimeStatus("UNKNOWN", "unknown-worker", false, 0, 0, 0, Instant.now()));
        }
        return Mono.fromSupplier(runtimeStatusService::health);
    }

    @GetMapping("/readiness")
    public Mono<JobGovernanceRuntimeStatusService.RuntimeStatus> readiness() {
        if (runtimeStatusService == null) {
            return Mono.just(new JobGovernanceRuntimeStatusService.RuntimeStatus("UNKNOWN", "unknown-worker", false, 0, 0, 0, Instant.now()));
        }
        return Mono.fromSupplier(runtimeStatusService::readiness);
    }

    @PostMapping("/jobs/{jobKey}/trigger")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<JobGovernanceManagementService.ExecutionView> triggerNow(
            @PathVariable("jobKey") String jobKey,
            @RequestBody JobGovernanceReactiveAdminController.TriggerRequest request
    ) {
        return Mono.fromSupplier(
                () -> managementService.triggerNow(
                        jobKey,
                        request.tenantId(),
                        request.actor(),
                        request.payloadJson(),
                        request.idempotencyKey()
                )
        );
    }

    @PostMapping("/executions/{executionId}/cancel")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> cancelExecution(
            @PathVariable("executionId") UUID executionId,
            @RequestBody CancelRequest request
    ) {
        return Mono.fromRunnable(() -> managementService.cancelExecution(executionId, request.actor(), request.reason()));
    }

    @GetMapping("/executions/retries")
    public Flux<JobGovernanceManagementService.ExecutionView> listRetryExecutions(
            @RequestParam(name = "tenantId", required = false) String tenantId,
            @RequestParam(name = "limit", defaultValue = "100") int limit
    ) {
        return Flux.fromIterable(managementService.listRetryExecutions(tenantId, limit));
    }

    @GetMapping("/executions/dead")
    public Flux<JobGovernanceManagementService.ExecutionView> listDeadExecutions(
            @RequestParam(name = "tenantId", required = false) String tenantId,
            @RequestParam(name = "limit", defaultValue = "100") int limit
    ) {
        return Flux.fromIterable(managementService.listDeadExecutions(tenantId, limit));
    }

    public record TriggerRequest(String tenantId, String actor, String payloadJson, String idempotencyKey) {
    }

    public record CancelRequest(String actor, String reason) {
    }
}
