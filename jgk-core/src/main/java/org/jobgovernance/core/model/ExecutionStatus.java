package org.jobgovernance.core.model;

public enum ExecutionStatus {
    REGISTERED,
    SCHEDULED,
    CLAIMED,
    RUNNING,
    SUCCEEDED,
    FAILED_RETRYABLE,
    FAILED_FINAL,
    TIMED_OUT,
    CANCEL_REQUESTED,
    CANCELLED,
    SKIPPED,
    MISFIRED,
    DEAD,
    PAUSED
}
