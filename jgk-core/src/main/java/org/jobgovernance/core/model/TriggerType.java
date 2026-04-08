package org.jobgovernance.core.model;

public enum TriggerType {
    IMMEDIATE,
    DELAYED,
    FIXED_DELAY,
    FIXED_RATE,
    CRON,
    MANUAL,
    RETRY,
    CATCH_UP,
    API
}
