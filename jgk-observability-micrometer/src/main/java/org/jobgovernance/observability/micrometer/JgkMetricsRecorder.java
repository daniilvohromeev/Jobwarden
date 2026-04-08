package org.jobgovernance.observability.micrometer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public final class JgkMetricsRecorder {

    private final Counter scheduled;
    private final Counter claimed;
    private final Counter succeeded;
    private final Counter failed;
    private final Counter timedOut;
    private final Counter cancelled;
    private final Counter retries;
    private final Counter claimContention;
    private final Counter leaseRenewalFailures;
    private final Timer executionDuration;
    private final Timer queueLag;
    private final Timer scheduleLateness;
    private final LongTaskTimer activeExecutions;

    public JgkMetricsRecorder(MeterRegistry meterRegistry) {
        this.scheduled = meterRegistry.counter("jgk.jobs.scheduled.total");
        this.claimed = meterRegistry.counter("jgk.jobs.claimed.total");
        this.succeeded = meterRegistry.counter("jgk.jobs.succeeded.total");
        this.failed = meterRegistry.counter("jgk.jobs.failed.total");
        this.timedOut = meterRegistry.counter("jgk.jobs.timed_out.total");
        this.cancelled = meterRegistry.counter("jgk.jobs.cancelled.total");
        this.retries = meterRegistry.counter("jgk.jobs.retries.total");
        this.claimContention = meterRegistry.counter("jgk.jobs.claim_contention.total");
        this.leaseRenewalFailures = meterRegistry.counter("jgk.jobs.lease_renewal_failures.total");
        this.executionDuration = meterRegistry.timer("jgk.jobs.execution.duration");
        this.queueLag = meterRegistry.timer("jgk.jobs.queue.lag");
        this.scheduleLateness = meterRegistry.timer("jgk.jobs.schedule.lateness");
        this.activeExecutions = LongTaskTimer.builder("jgk.jobs.running").register(meterRegistry);
    }

    public Counter scheduledCounter() {
        return scheduled;
    }

    public Counter claimedCounter() {
        return claimed;
    }

    public Counter succeededCounter() {
        return succeeded;
    }

    public Counter failedCounter() {
        return failed;
    }

    public Counter timedOutCounter() {
        return timedOut;
    }

    public Counter cancelledCounter() {
        return cancelled;
    }

    public Counter retriesCounter() {
        return retries;
    }

    public Counter claimContentionCounter() {
        return claimContention;
    }

    public Counter leaseRenewalFailuresCounter() {
        return leaseRenewalFailures;
    }

    public Timer executionDurationTimer() {
        return executionDuration;
    }

    public Timer queueLagTimer() {
        return queueLag;
    }

    public Timer scheduleLatenessTimer() {
        return scheduleLateness;
    }

    public LongTaskTimer activeExecutionsTimer() {
        return activeExecutions;
    }
}
