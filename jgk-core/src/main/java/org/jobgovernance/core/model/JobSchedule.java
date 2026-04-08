package org.jobgovernance.core.model;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;

public sealed interface JobSchedule permits
        JobSchedule.OneTimeSchedule,
        JobSchedule.FixedDelaySchedule,
        JobSchedule.FixedRateSchedule,
        JobSchedule.CronSchedule,
        JobSchedule.DisabledSchedule {

    Kind kind();

    ZoneId zoneId();

    Instant effectiveFrom();

    Instant effectiveTo();

    enum Kind {
        ONE_TIME,
        FIXED_DELAY,
        FIXED_RATE,
        CRON,
        DISABLED
    }

    record OneTimeSchedule(
            Instant scheduledAt,
            ZoneId zoneId,
            Instant effectiveFrom,
            Instant effectiveTo
    ) implements JobSchedule {
        public OneTimeSchedule {
            if (scheduledAt == null) {
                throw new IllegalArgumentException("scheduledAt is required");
            }
            zoneId = zoneId == null ? ZoneId.of("UTC") : zoneId;
        }

        @Override
        public Kind kind() {
            return Kind.ONE_TIME;
        }
    }

    record FixedDelaySchedule(
            Duration delay,
            Duration initialDelay,
            ZoneId zoneId,
            Instant effectiveFrom,
            Instant effectiveTo
    ) implements JobSchedule {
        public FixedDelaySchedule {
            if (delay == null || delay.isNegative() || delay.isZero()) {
                throw new IllegalArgumentException("delay must be positive");
            }
            initialDelay = initialDelay == null ? Duration.ZERO : initialDelay;
            zoneId = zoneId == null ? ZoneId.of("UTC") : zoneId;
        }

        @Override
        public Kind kind() {
            return Kind.FIXED_DELAY;
        }
    }

    record FixedRateSchedule(
            Duration rate,
            Duration initialDelay,
            ZoneId zoneId,
            Instant effectiveFrom,
            Instant effectiveTo
    ) implements JobSchedule {
        public FixedRateSchedule {
            if (rate == null || rate.isNegative() || rate.isZero()) {
                throw new IllegalArgumentException("rate must be positive");
            }
            initialDelay = initialDelay == null ? Duration.ZERO : initialDelay;
            zoneId = zoneId == null ? ZoneId.of("UTC") : zoneId;
        }

        @Override
        public Kind kind() {
            return Kind.FIXED_RATE;
        }
    }

    record CronSchedule(
            String cronExpression,
            ZoneId zoneId,
            Instant effectiveFrom,
            Instant effectiveTo
    ) implements JobSchedule {
        public CronSchedule {
            if (cronExpression == null || cronExpression.isBlank()) {
                throw new IllegalArgumentException("cronExpression is required");
            }
            zoneId = zoneId == null ? ZoneId.of("UTC") : zoneId;
        }

        @Override
        public Kind kind() {
            return Kind.CRON;
        }
    }

    record DisabledSchedule(ZoneId zoneId, Instant effectiveFrom, Instant effectiveTo) implements JobSchedule {
        public DisabledSchedule {
            zoneId = zoneId == null ? ZoneId.of("UTC") : zoneId;
        }

        @Override
        public Kind kind() {
            return Kind.DISABLED;
        }
    }

    default boolean isWithinEffectiveWindow(Instant instant) {
        if (instant == null) {
            return false;
        }
        if (effectiveFrom() != null && instant.isBefore(effectiveFrom())) {
            return false;
        }
        return effectiveTo() == null || !instant.isAfter(effectiveTo());
    }

    default Optional<Instant> maybeOneTime() {
        if (this instanceof OneTimeSchedule oneTimeSchedule) {
            return Optional.of(oneTimeSchedule.scheduledAt());
        }
        return Optional.empty();
    }
}
