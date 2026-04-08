package org.jobgovernance.core.model;

import java.util.Set;

public record JobDefinition(
        String jobKey,
        int version,
        String displayName,
        String description,
        String ownerTeam,
        Set<String> tags,
        ExecutionMode executionMode,
        JobSchedule schedule,
        JobPolicy policy,
        String payloadSchemaVersion,
        JobDefinitionState state,
        boolean manualTriggerable,
        boolean internalOnly,
        String tenantScope
) {
    public JobDefinition {
        if (jobKey == null || jobKey.isBlank()) {
            throw new IllegalArgumentException("jobKey is required");
        }
        if (version < 1) {
            throw new IllegalArgumentException("version must be >= 1");
        }
        if (executionMode == null) {
            throw new IllegalArgumentException("executionMode is required");
        }
        if (schedule == null) {
            throw new IllegalArgumentException("schedule is required");
        }
        if (policy == null) {
            throw new IllegalArgumentException("policy is required");
        }
        if (state == null) {
            throw new IllegalArgumentException("state is required");
        }
        tags = tags == null ? Set.of() : Set.copyOf(tags);
    }
}
