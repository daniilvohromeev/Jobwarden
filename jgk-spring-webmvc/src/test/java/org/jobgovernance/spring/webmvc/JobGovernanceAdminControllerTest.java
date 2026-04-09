package org.jobgovernance.spring.webmvc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jobgovernance.spring.core.JobGovernanceManagementService;
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
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        managementService = mock(JobGovernanceManagementService.class);
        objectMapper = new ObjectMapper();
        mockMvc = MockMvcBuilders.standaloneSetup(new JobGovernanceAdminController(managementService)).build();
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
}
