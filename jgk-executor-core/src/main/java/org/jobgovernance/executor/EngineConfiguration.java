package org.jobgovernance.executor;

import java.time.Duration;

public record EngineConfiguration(
        int scheduleBatchSize,
        int claimBatchSize,
        int maxClaimedNotStarted,
        int maxRunningGlobal,
        Duration emptyPollBackoffMin,
        Duration emptyPollBackoffMax,
        Duration heartbeatInterval,
        Duration recoveryInterval,
        Duration cleanupInterval
) {
    public EngineConfiguration {
        if (scheduleBatchSize < 1 || claimBatchSize < 1) {
            throw new IllegalArgumentException("batch sizes must be >= 1");
        }
        if (maxClaimedNotStarted < 1 || maxRunningGlobal < 1) {
            throw new IllegalArgumentException("running limits must be >= 1");
        }
        emptyPollBackoffMin = emptyPollBackoffMin == null ? Duration.ofMillis(200) : emptyPollBackoffMin;
        emptyPollBackoffMax = emptyPollBackoffMax == null ? Duration.ofSeconds(5) : emptyPollBackoffMax;
        heartbeatInterval = heartbeatInterval == null ? Duration.ofSeconds(30) : heartbeatInterval;
        recoveryInterval = recoveryInterval == null ? Duration.ofSeconds(30) : recoveryInterval;
        cleanupInterval = cleanupInterval == null ? Duration.ofMinutes(10) : cleanupInterval;
    }
}
