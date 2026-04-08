package org.jobgovernance.core.api;

import org.jobgovernance.core.model.JobExecution;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface ClaimService {

    List<JobExecution> claimDueExecutions(String workerId, int batchSize, Instant now);

    boolean requestCancel(UUID executionId, String actor, String reason, Instant now);
}
