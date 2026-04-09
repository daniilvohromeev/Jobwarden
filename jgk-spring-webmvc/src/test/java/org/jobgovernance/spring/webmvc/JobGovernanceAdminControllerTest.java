package org.jobgovernance.spring.webmvc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jobgovernance.spring.core.JobGovernanceManagementService;
import org.jobgovernance.spring.core.JobGovernanceRuntimeStatusService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class JobGovernanceAdminControllerTest {

    private MockMvc mockMvc;

    private JobGovernanceManagementService managementService;
    private JobGovernanceRuntimeStatusService runtimeStatusService;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        managementService = mock(JobGovernanceManagementService.class);
        runtimeStatusService = mock(JobGovernanceRuntimeStatusService.class);
        objectMapper = new ObjectMapper();
        mockMvc = MockMvcBuilders.standaloneSetup(new JobGovernanceAdminController(managementService, runtimeStatusService)).build();
    }

    @Test
    void listJobsShouldReturnRegisteredJobs() throws Exception {
        when(managementService.listJobs("tenant-a", 2)).thenReturn(List.of(
                new JobGovernanceManagementService.JobView(
                        "billing.reconcile",
                        "Billing Reconcile",
                        "ENABLED",
                        "billing-team",
                        List.of("billing", "nightly")
                )
        ));

        mockMvc.perform(get("/jgk/v1/jobs")
                        .param("tenantId", "tenant-a")
                        .param("limit", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].jobKey").value("billing.reconcile"))
                .andExpect(jsonPath("$[0].displayName").value("Billing Reconcile"))
                .andExpect(jsonPath("$[0].state").value("ENABLED"));

        verify(managementService).listJobs("tenant-a", 2);
    }

    @Test
    void triggerNowShouldReturnAcceptedExecution() throws Exception {
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

        mockMvc.perform(post("/jgk/v1/jobs/{jobKey}/trigger", "billing.reconcile")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsBytes(new JobGovernanceAdminController.TriggerRequest(
                                "tenant-a",
                                "ops-user",
                                "{\"force\":true}",
                                "idem-1"
                        ))))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.executionId").value(executionId.toString()))
                .andExpect(jsonPath("$.status").value("SCHEDULED"));

        verify(managementService).triggerNow("billing.reconcile", "tenant-a", "ops-user", "{\"force\":true}", "idem-1");
    }

    @Test
    void cancelExecutionShouldReturnNoContent() throws Exception {
        UUID executionId = UUID.randomUUID();

        mockMvc.perform(post("/jgk/v1/executions/{executionId}/cancel", executionId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsBytes(new JobGovernanceAdminController.CancelRequest(
                                "ops-user",
                                "manual stop"
                        ))))
                .andExpect(status().isNoContent());

        verify(managementService).cancelExecution(executionId, "ops-user", "manual stop");
    }

    @Test
    void retryExecutionShouldReturnAcceptedExecution() throws Exception {
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

        mockMvc.perform(post("/jgk/v1/executions/{executionId}/retry", sourceExecutionId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsBytes(new JobGovernanceAdminController.RetryRequest("ops-user"))))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.executionId").value(retryExecutionId.toString()))
                .andExpect(jsonPath("$.status").value("SCHEDULED"));

        verify(managementService).retryExecution(sourceExecutionId, "ops-user");
    }

    @Test
    void healthShouldExposeRuntimeStatus() throws Exception {
        when(runtimeStatusService.health()).thenReturn(new JobGovernanceRuntimeStatusService.RuntimeStatus(
                "UP",
                "worker-a",
                true,
                6,
                2,
                14,
                Instant.parse("2026-01-01T00:00:00Z")
        ));

        mockMvc.perform(get("/jgk/v1/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("UP"))
                .andExpect(jsonPath("$.workerId").value("worker-a"))
                .andExpect(jsonPath("$.engineRunning").value(true));

        verify(runtimeStatusService).health();
    }

    @Test
    void getJobShouldReturnSingleJobView() throws Exception {
        when(managementService.getJob("billing.reconcile", "tenant-a")).thenReturn(java.util.Optional.of(
                new JobGovernanceManagementService.JobView(
                        "billing.reconcile",
                        "Billing Reconcile",
                        "ENABLED",
                        "billing-team",
                        List.of("billing")
                )
        ));

        mockMvc.perform(get("/jgk/v1/jobs/{jobKey}", "billing.reconcile")
                        .param("tenantId", "tenant-a"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.jobKey").value("billing.reconcile"))
                .andExpect(jsonPath("$.state").value("ENABLED"));

        verify(managementService).getJob("billing.reconcile", "tenant-a");
    }

    @Test
    void listDeadExecutionsShouldReturnPayload() throws Exception {
        UUID executionId = UUID.randomUUID();
        when(managementService.listDeadExecutions("tenant-a", 5)).thenReturn(List.of(
                new JobGovernanceManagementService.ExecutionView(
                        executionId,
                        "billing.reconcile",
                        "DEAD",
                        Instant.parse("2026-01-01T00:00:00Z"),
                        null,
                        Instant.parse("2026-01-01T00:02:00Z"),
                        3,
                        "worker-a",
                        Map.of("self", "/jgk/v1/executions/" + executionId)
                )
        ));

        mockMvc.perform(get("/jgk/v1/executions/dead")
                        .param("tenantId", "tenant-a")
                        .param("limit", "5"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].executionId").value(executionId.toString()))
                .andExpect(jsonPath("$[0].status").value("DEAD"));

        verify(managementService).listDeadExecutions("tenant-a", 5);
    }

    @Test
    void listJobAuditEventsShouldReturnPayload() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID executionId = UUID.randomUUID();
        when(managementService.listAuditEventsByJob("billing.reconcile", 5)).thenReturn(List.of(
                new JobGovernanceManagementService.AuditView(
                        eventId,
                        "MANUAL_TRIGGER_ENQUEUED",
                        "billing.reconcile",
                        executionId,
                        "operator",
                        "{\"tenantId\":\"\"}",
                        Instant.parse("2026-01-01T00:00:00Z")
                )
        ));

        mockMvc.perform(get("/jgk/v1/jobs/{jobKey}/audit", "billing.reconcile")
                        .param("limit", "5"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].eventId").value(eventId.toString()))
                .andExpect(jsonPath("$[0].eventType").value("MANUAL_TRIGGER_ENQUEUED"));

        verify(managementService).listAuditEventsByJob("billing.reconcile", 5);
    }

    @Test
    void listActiveWorkersShouldReturnPayload() throws Exception {
        when(managementService.listActiveWorkers(5)).thenReturn(List.of(
                new JobGovernanceManagementService.WorkerView(
                        "worker-a",
                        3,
                        Instant.parse("2026-01-01T00:00:00Z"),
                        Instant.parse("2026-01-01T00:00:10Z"),
                        Instant.parse("2026-01-01T00:00:30Z")
                )
        ));

        mockMvc.perform(get("/jgk/v1/workers/active")
                        .param("limit", "5"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].workerId").value("worker-a"))
                .andExpect(jsonPath("$[0].activeExecutions").value(3));

        verify(managementService).listActiveWorkers(5);
    }
}
