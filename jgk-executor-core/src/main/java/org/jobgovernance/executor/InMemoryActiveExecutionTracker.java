package org.jobgovernance.executor;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class InMemoryActiveExecutionTracker implements ActiveExecutionTracker {

    private final ConcurrentMap<UUID, ActiveExecution> activeExecutions = new ConcurrentHashMap<>();

    @Override
    public void add(ActiveExecution activeExecution) {
        activeExecutions.put(activeExecution.executionId(), activeExecution);
    }

    @Override
    public void remove(UUID executionId) {
        activeExecutions.remove(executionId);
    }

    @Override
    public Collection<ActiveExecution> snapshot() {
        return List.copyOf(activeExecutions.values());
    }
}
