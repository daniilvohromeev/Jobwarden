package org.jobgovernance.core.api;

import org.jobgovernance.core.model.JobDefinition;

import java.time.Instant;
import java.util.List;

public interface ScheduleEvaluator {

    EvaluationResult evaluate(JobDefinition definition, ScheduleCursor cursor, Instant now, int maxBatch);

    default List<DueExecutionCandidate> evaluateDue(JobDefinition definition, ScheduleCursor cursor, Instant now, int maxBatch) {
        return evaluate(definition, cursor, now, maxBatch).dueCandidates();
    }

    record EvaluationResult(
            List<DueExecutionCandidate> dueCandidates,
            Instant nextFireAt
    ) {
    }

    record ScheduleCursor(String jobKey, Instant lastEvaluatedAt, Instant nextFireAt) {
    }

    record DueExecutionCandidate(String jobKey, Instant scheduledAt, String triggerType, String dedupeKey) {
    }
}
