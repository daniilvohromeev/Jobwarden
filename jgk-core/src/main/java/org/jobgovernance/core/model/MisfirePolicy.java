package org.jobgovernance.core.model;

public enum MisfirePolicy {
    IGNORE,
    RUN_ONCE_IMMEDIATELY,
    CATCH_UP_ALL_MISSED_WINDOWS,
    CATCH_UP_LATEST_ONLY,
    MARK_MISFIRED_AND_SKIP
}
