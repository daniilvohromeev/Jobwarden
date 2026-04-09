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
}
