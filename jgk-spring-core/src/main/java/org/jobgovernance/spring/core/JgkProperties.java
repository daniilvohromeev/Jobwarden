package org.jobgovernance.spring.core;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "jgk")
public record JgkProperties(
        boolean enabled,
        boolean autoStart,
        String workerId,
        int scheduleBatchSize,
        int claimBatchSize,
        int maxClaimedNotStarted,
        int maxRunningGlobal,
        int retryRequeueBatchSize,
        Duration leaseTtl,
        Duration schedulerInterval,
        Duration claimPollBackoffMin,
        Duration claimPollBackoffMax,
        Duration heartbeatInterval,
        Duration retryRequeueInterval,
        Duration recoveryInterval,
        Duration cleanupInterval,
        Duration cleanupRetention,
        int cleanupBatchSize,
        String recoveryDeadReason,
        Duration shutdownGracePeriod
) {
    public JgkProperties {
        workerId = workerId == null || workerId.isBlank() ? "unknown-worker" : workerId;
        scheduleBatchSize = scheduleBatchSize <= 0 ? 128 : scheduleBatchSize;
        claimBatchSize = claimBatchSize <= 0 ? 64 : claimBatchSize;
        maxClaimedNotStarted = maxClaimedNotStarted <= 0 ? 256 : maxClaimedNotStarted;
        maxRunningGlobal = maxRunningGlobal <= 0 ? 32 : maxRunningGlobal;
        retryRequeueBatchSize = retryRequeueBatchSize <= 0 ? 128 : retryRequeueBatchSize;
        leaseTtl = leaseTtl == null ? Duration.ofSeconds(45) : leaseTtl;
        schedulerInterval = schedulerInterval == null ? Duration.ofSeconds(5) : schedulerInterval;
        claimPollBackoffMin = claimPollBackoffMin == null ? Duration.ofMillis(200) : claimPollBackoffMin;
        claimPollBackoffMax = claimPollBackoffMax == null ? Duration.ofSeconds(5) : claimPollBackoffMax;
        heartbeatInterval = heartbeatInterval == null ? Duration.ofSeconds(30) : heartbeatInterval;
        retryRequeueInterval = retryRequeueInterval == null ? Duration.ofSeconds(5) : retryRequeueInterval;
        recoveryInterval = recoveryInterval == null ? Duration.ofSeconds(30) : recoveryInterval;
        cleanupInterval = cleanupInterval == null ? Duration.ofMinutes(10) : cleanupInterval;
        cleanupRetention = cleanupRetention == null ? Duration.ofDays(30) : cleanupRetention;
        cleanupBatchSize = cleanupBatchSize <= 0 ? 1_000 : cleanupBatchSize;
        recoveryDeadReason = recoveryDeadReason == null || recoveryDeadReason.isBlank()
                ? "recovery deadline exceeded"
                : recoveryDeadReason;
        shutdownGracePeriod = shutdownGracePeriod == null ? Duration.ofSeconds(30) : shutdownGracePeriod;
    }
}
