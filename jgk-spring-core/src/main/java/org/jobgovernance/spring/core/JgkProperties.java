package org.jobgovernance.spring.core;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "jgk")
public record JgkProperties(
        boolean enabled,
        String workerId,
        int scheduleBatchSize,
        int claimBatchSize,
        int maxRunningGlobal,
        Duration heartbeatInterval,
        Duration shutdownGracePeriod
) {
    public JgkProperties {
        workerId = workerId == null || workerId.isBlank() ? "unknown-worker" : workerId;
        scheduleBatchSize = scheduleBatchSize <= 0 ? 128 : scheduleBatchSize;
        claimBatchSize = claimBatchSize <= 0 ? 64 : claimBatchSize;
        maxRunningGlobal = maxRunningGlobal <= 0 ? 32 : maxRunningGlobal;
        heartbeatInterval = heartbeatInterval == null ? Duration.ofSeconds(30) : heartbeatInterval;
        shutdownGracePeriod = shutdownGracePeriod == null ? Duration.ofSeconds(30) : shutdownGracePeriod;
    }
}
