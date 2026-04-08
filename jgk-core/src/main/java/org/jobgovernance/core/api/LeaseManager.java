package org.jobgovernance.core.api;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

public interface LeaseManager {

    boolean renewLease(UUID executionId, String workerId, String leaseToken, Duration extension, Instant now);

    boolean releaseLease(UUID executionId, String workerId, String leaseToken, Instant now);
}
