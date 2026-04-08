package org.jobgovernance.spring.core;

import org.jobgovernance.executor.ExecutionEngine;
import org.springframework.context.SmartLifecycle;

import java.util.concurrent.atomic.AtomicBoolean;

final class ExecutionEngineLifecycle implements SmartLifecycle {

    private static final int PHASE = Integer.MAX_VALUE - 100;

    private final ExecutionEngine executionEngine;
    private final AtomicBoolean running = new AtomicBoolean(false);

    ExecutionEngineLifecycle(ExecutionEngine executionEngine) {
        this.executionEngine = executionEngine;
    }

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            executionEngine.start();
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            executionEngine.stop();
        }
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }
}
