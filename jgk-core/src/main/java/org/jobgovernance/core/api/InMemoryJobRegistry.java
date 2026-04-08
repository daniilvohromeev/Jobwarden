package org.jobgovernance.core.api;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class InMemoryJobRegistry implements JobRegistry {

    private final ConcurrentMap<String, JobRegistration<?, ?>> registrations = new ConcurrentHashMap<>();

    @Override
    public <P, R> void register(JobRegistration<P, R> registration) {
        registrations.compute(registration.definition().jobKey(), (jobKey, current) -> {
            if (current == null) {
                return registration;
            }
            int currentVersion = current.definition().version();
            int newVersion = registration.definition().version();
            if (newVersion <= currentVersion) {
                throw new IllegalStateException(
                        "Job key '%s' already registered with version %d; incoming version must be greater than current"
                                .formatted(jobKey, currentVersion)
                );
            }
            return registration;
        });
    }

    @Override
    public Optional<JobRegistration<?, ?>> findByJobKey(String jobKey) {
        return Optional.ofNullable(registrations.get(jobKey));
    }

    @Override
    public Collection<JobRegistration<?, ?>> all() {
        return List.copyOf(registrations.values());
    }
}
