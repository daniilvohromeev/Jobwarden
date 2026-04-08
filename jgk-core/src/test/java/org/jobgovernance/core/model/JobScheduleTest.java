package org.jobgovernance.core.model;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertThrows;

class JobScheduleTest {

    @Test
    void fixedRateMustBePositive() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new JobSchedule.FixedRateSchedule(Duration.ZERO, Duration.ZERO, ZoneId.of("UTC"), null, null)
        );
    }
}
