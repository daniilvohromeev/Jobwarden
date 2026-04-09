package org.jobgovernance.spring.core;

import org.jobgovernance.core.api.InMemoryJobRegistry;
import org.jobgovernance.executor.ActiveExecutionTracker;
import org.jobgovernance.executor.InMemoryActiveExecutionTracker;
import org.jobgovernance.executor.ExecutionEngine;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultJobGovernanceRuntimeStatusServiceTest {

    @Test
    void shouldReportDownAndNotReadyWhenEngineStopped() {
        ExecutionEngine engine = new ExecutionEngine(List.of());
        InMemoryActiveExecutionTracker activeExecutionTracker = new InMemoryActiveExecutionTracker();
        InMemoryJobRegistry jobRegistry = new InMemoryJobRegistry();
        Clock clock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);

        DefaultJobGovernanceRuntimeStatusService statusService = new DefaultJobGovernanceRuntimeStatusService(
                engine,
                activeExecutionTracker,
                jobRegistry,
                "worker-a",
                clock
        );

        JobGovernanceRuntimeStatusService.RuntimeStatus health = statusService.health();
        JobGovernanceRuntimeStatusService.RuntimeStatus readiness = statusService.readiness();

        assertEquals("DOWN", health.state());
        assertFalse(health.engineRunning());
        assertEquals("NOT_READY", readiness.state());
        assertFalse(readiness.engineRunning());
    }

    @Test
    void shouldReportUpAndReadyWhenEngineRunning() {
        ExecutionEngine engine = new ExecutionEngine(List.of(
                new NoopLoop(),
                new NoopLoop()
        ));
        InMemoryActiveExecutionTracker activeExecutionTracker = new InMemoryActiveExecutionTracker();
        activeExecutionTracker.add(new ActiveExecutionTracker.ActiveExecution(
                UUID.randomUUID(),
                "worker-a",
                "lease-1",
                Duration.ofSeconds(30)
        ));
        InMemoryJobRegistry jobRegistry = new InMemoryJobRegistry();
        Clock clock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);

        DefaultJobGovernanceRuntimeStatusService statusService = new DefaultJobGovernanceRuntimeStatusService(
                engine,
                activeExecutionTracker,
                jobRegistry,
                "worker-a",
                clock
        );
        engine.start();

        JobGovernanceRuntimeStatusService.RuntimeStatus health = statusService.health();
        JobGovernanceRuntimeStatusService.RuntimeStatus readiness = statusService.readiness();

        assertEquals("UP", health.state());
        assertTrue(health.engineRunning());
        assertEquals(2, health.loopCount());
        assertEquals(1, health.activeExecutions());

        assertEquals("READY", readiness.state());
        assertTrue(readiness.engineRunning());
        assertEquals(2, readiness.loopCount());
        assertEquals(1, readiness.activeExecutions());
    }

    private static final class NoopLoop implements ExecutionEngine.RunnableLoop {
        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }
    }
}
