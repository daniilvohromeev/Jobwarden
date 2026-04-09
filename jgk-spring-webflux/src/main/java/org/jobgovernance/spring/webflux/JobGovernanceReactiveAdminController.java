package org.jobgovernance.spring.webflux;

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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@RequestMapping("/jgk/v1")
public class JobGovernanceReactiveAdminController {

    private final JobGovernanceManagementService managementService;

    public JobGovernanceReactiveAdminController(JobGovernanceManagementService managementService) {
        this.managementService = managementService;
    }

    @GetMapping("/jobs")
    public Flux<JobGovernanceManagementService.JobView> listJobs(
            @RequestParam(name = "tenantId", required = false) String tenantId,
            @RequestParam(name = "limit", defaultValue = "100") int limit
    ) {
        return Flux.fromIterable(managementService.listJobs(tenantId, limit));
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

    public record TriggerRequest(String tenantId, String actor, String payloadJson, String idempotencyKey) {
    }

    public record CancelRequest(String actor, String reason) {
    }
}
