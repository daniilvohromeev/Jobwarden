package org.jobgovernance.spring.webflux;

import org.jobgovernance.spring.core.JobGovernanceManagementService;
import org.jobgovernance.spring.core.JobGovernanceRuntimeStatusService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JobGovernanceReactiveAdminControllerTest {

    private WebTestClient webTestClient;
    private JobGovernanceManagementService managementService;
    private JobGovernanceRuntimeStatusService runtimeStatusService;

    @BeforeEach
    void setUp() {
        managementService = mock(JobGovernanceManagementService.class);
        runtimeStatusService = mock(JobGovernanceRuntimeStatusService.class);
        webTestClient = WebTestClient.bindToController(
                new JobGovernanceReactiveAdminController(managementService, runtimeStatusService)
        ).build();
    }

    @Test
    void listJobsShouldReturnFluxPayload() {
        when(managementService.listJobs("tenant-a", 2)).thenReturn(List.of(
                new JobGovernanceManagementService.JobView(
                        "billing.reconcile",
                        "Billing Reconcile",
                        "ENABLED",
                        "billing-team",
                        List.of("billing", "nightly")
                )
        ));

        webTestClient.get()
                .uri(uriBuilder -> uriBuilder.path("/jgk/v1/jobs")
                        .queryParam("tenantId", "tenant-a")
                        .queryParam("limit", 2)
                        .build())
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$[0].jobKey").isEqualTo("billing.reconcile")
                .jsonPath("$[0].state").isEqualTo("ENABLED");

        verify(managementService).listJobs("tenant-a", 2);
    }

    @Test
    void triggerNowShouldReturnAcceptedExecution() {
        UUID executionId = UUID.randomUUID();
        when(managementService.triggerNow(
                "billing.reconcile",
                "tenant-a",
                "ops-user",
                "{\"force\":true}",
                "idem-1"
        )).thenReturn(new JobGovernanceManagementService.ExecutionView(
                executionId,
                "billing.reconcile",
                "SCHEDULED",
                Instant.parse("2026-01-01T00:00:00Z"),
                null,
                null,
                1,
                null,
                Map.of("self", "/jgk/v1/executions/" + executionId)
        ));

        webTestClient.post()
                .uri("/jgk/v1/jobs/{jobKey}/trigger", "billing.reconcile")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new JobGovernanceReactiveAdminController.TriggerRequest(
                        "tenant-a",
                        "ops-user",
                        "{\"force\":true}",
                        "idem-1"
                ))
                .exchange()
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.executionId").isEqualTo(executionId.toString())
                .jsonPath("$.status").isEqualTo("SCHEDULED");

        verify(managementService).triggerNow("billing.reconcile", "tenant-a", "ops-user", "{\"force\":true}", "idem-1");
    }

    @Test
    void cancelShouldReturnNoContent() {
        UUID executionId = UUID.randomUUID();

        webTestClient.post()
                .uri("/jgk/v1/executions/{executionId}/cancel", executionId)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new JobGovernanceReactiveAdminController.CancelRequest(
                        "ops-user",
                        "manual stop"
                ))
                .exchange()
                .expectStatus().isNoContent();

        verify(managementService).cancelExecution(executionId, "ops-user", "manual stop");
    }

    @Test
    void retryShouldReturnAcceptedExecution() {
        UUID sourceExecutionId = UUID.randomUUID();
        UUID retryExecutionId = UUID.randomUUID();
        when(managementService.retryExecution(sourceExecutionId, "ops-user")).thenReturn(
                new JobGovernanceManagementService.ExecutionView(
                        retryExecutionId,
                        "billing.reconcile",
                        "SCHEDULED",
                        Instant.parse("2026-01-01T00:00:30Z"),
                        null,
                        null,
                        1,
                        null,
                        Map.of("self", "/jgk/v1/executions/" + retryExecutionId)
                )
        );

        webTestClient.post()
                .uri("/jgk/v1/executions/{executionId}/retry", sourceExecutionId)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new JobGovernanceReactiveAdminController.RetryRequest("ops-user"))
                .exchange()
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.executionId").isEqualTo(retryExecutionId.toString())
                .jsonPath("$.status").isEqualTo("SCHEDULED");

        verify(managementService).retryExecution(sourceExecutionId, "ops-user");
    }

    @Test
    void triggerAtShouldReturnAcceptedExecution() {
        UUID executionId = UUID.randomUUID();
        Instant triggerAt = Instant.parse("2026-01-01T00:05:00Z");
        when(managementService.triggerAt(
                "billing.reconcile",
                "tenant-a",
                "ops-user",
                "{\"force\":true}",
                triggerAt,
                "idem-2"
        )).thenReturn(new JobGovernanceManagementService.ExecutionView(
                executionId,
                "billing.reconcile",
                "SCHEDULED",
                triggerAt,
                null,
                null,
                1,
                null,
                Map.of("self", "/jgk/v1/executions/" + executionId)
        ));

        webTestClient.post()
                .uri("/jgk/v1/jobs/{jobKey}/trigger-at", "billing.reconcile")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new JobGovernanceReactiveAdminController.TriggerAtRequest(
                        "tenant-a",
                        "ops-user",
                        "{\"force\":true}",
                        triggerAt,
                        "idem-2"
                ))
                .exchange()
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.executionId").isEqualTo(executionId.toString())
                .jsonPath("$.status").isEqualTo("SCHEDULED");

        verify(managementService).triggerAt("billing.reconcile", "tenant-a", "ops-user", "{\"force\":true}", triggerAt, "idem-2");
    }

    @Test
    void readinessShouldExposeRuntimeStatus() {
        when(runtimeStatusService.readiness()).thenReturn(new JobGovernanceRuntimeStatusService.RuntimeStatus(
                "READY",
                "worker-a",
                true,
                6,
                1,
                12,
                Instant.parse("2026-01-01T00:00:00Z")
        ));

        webTestClient.get()
                .uri("/jgk/v1/readiness")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.state").isEqualTo("READY")
                .jsonPath("$.workerId").isEqualTo("worker-a")
                .jsonPath("$.engineRunning").isEqualTo(true);

        verify(runtimeStatusService).readiness();
    }

    @Test
    void getJobShouldReturnSingleJobView() {
        when(managementService.getJob("billing.reconcile", "tenant-a")).thenReturn(java.util.Optional.of(
                new JobGovernanceManagementService.JobView(
                        "billing.reconcile",
                        "Billing Reconcile",
                        "ENABLED",
                        "billing-team",
                        List.of("billing")
                )
        ));

        webTestClient.get()
                .uri(uriBuilder -> uriBuilder.path("/jgk/v1/jobs/{jobKey}")
                        .queryParam("tenantId", "tenant-a")
                        .build("billing.reconcile"))
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.jobKey").isEqualTo("billing.reconcile")
                .jsonPath("$.state").isEqualTo("ENABLED");

        verify(managementService).getJob("billing.reconcile", "tenant-a");
    }

    @Test
    void listRetryExecutionsShouldReturnPayload() {
        UUID executionId = UUID.randomUUID();
        when(managementService.listRetryExecutions("tenant-a", 5)).thenReturn(List.of(
                new JobGovernanceManagementService.ExecutionView(
                        executionId,
                        "billing.reconcile",
                        "FAILED_RETRYABLE",
                        Instant.parse("2026-01-01T00:00:00Z"),
                        Instant.parse("2026-01-01T00:00:10Z"),
                        Instant.parse("2026-01-01T00:00:20Z"),
                        2,
                        "worker-a",
                        Map.of("self", "/jgk/v1/executions/" + executionId)
                )
        ));

        webTestClient.get()
                .uri(uriBuilder -> uriBuilder.path("/jgk/v1/executions/retries")
                        .queryParam("tenantId", "tenant-a")
                        .queryParam("limit", 5)
                        .build())
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$[0].executionId").isEqualTo(executionId.toString())
                .jsonPath("$[0].status").isEqualTo("FAILED_RETRYABLE");

        verify(managementService).listRetryExecutions("tenant-a", 5);
    }

    @Test
    void listExecutionAuditShouldReturnPayload() {
        UUID eventId = UUID.randomUUID();
        UUID executionId = UUID.randomUUID();
        when(managementService.listAuditEventsByExecution(executionId, 5)).thenReturn(List.of(
                new JobGovernanceManagementService.AuditView(
                        eventId,
                        "EXECUTION_CANCEL_REQUESTED",
                        "billing.reconcile",
                        executionId,
                        "operator",
                        "{\"reason\":\"manual\"}",
                        Instant.parse("2026-01-01T00:00:00Z")
                )
        ));

        webTestClient.get()
                .uri(uriBuilder -> uriBuilder.path("/jgk/v1/executions/{executionId}/audit")
                        .queryParam("limit", 5)
                        .build(executionId))
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$[0].eventId").isEqualTo(eventId.toString())
                .jsonPath("$[0].eventType").isEqualTo("EXECUTION_CANCEL_REQUESTED");

        verify(managementService).listAuditEventsByExecution(executionId, 5);
    }

    @Test
    void listActiveWorkersShouldReturnPayload() {
        when(managementService.listActiveWorkers(5)).thenReturn(List.of(
                new JobGovernanceManagementService.WorkerView(
                        "worker-a",
                        2,
                        Instant.parse("2026-01-01T00:00:00Z"),
                        Instant.parse("2026-01-01T00:00:10Z"),
                        Instant.parse("2026-01-01T00:00:40Z")
                )
        ));

        webTestClient.get()
                .uri(uriBuilder -> uriBuilder.path("/jgk/v1/workers/active")
                        .queryParam("limit", 5)
                        .build())
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$[0].workerId").isEqualTo("worker-a")
                .jsonPath("$[0].activeExecutions").isEqualTo(2);

        verify(managementService).listActiveWorkers(5);
    }
}
