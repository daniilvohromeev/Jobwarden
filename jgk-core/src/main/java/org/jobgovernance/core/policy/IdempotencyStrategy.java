package org.jobgovernance.core.policy;

import org.jobgovernance.core.api.ExecutionContext;

import java.util.Optional;

public interface IdempotencyStrategy {

    Optional<String> deduplicationKey(ExecutionContext<?> context);

    BeforeExecutionDecision beforeExecution(ExecutionContext<?> context, String deduplicationKey);

    void afterExecution(ExecutionContext<?> context, String deduplicationKey, AfterExecutionResult result);

    enum BeforeExecutionDecision {
        EXECUTE,
        SKIP_ALREADY_PROCESSED,
        REUSE_PREVIOUS_RESULT
    }

    record AfterExecutionResult(boolean success, String resultSummary, String errorSummary) {
    }
}
