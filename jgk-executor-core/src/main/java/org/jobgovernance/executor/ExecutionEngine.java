package org.jobgovernance.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ExecutionEngine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ExecutionEngine.class);

    private final List<RunnableLoop> loops;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public ExecutionEngine(List<RunnableLoop> loops) {
        this.loops = List.copyOf(loops);
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        loops.forEach(RunnableLoop::start);
        log.info("JGK execution engine started with {} loops", loops.size());
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        loops.forEach(RunnableLoop::stop);
        log.info("JGK execution engine stopped");
    }

    public boolean isRunning() {
        return started.get();
    }

    public int loopCount() {
        return loops.size();
    }

    @Override
    public void close() {
        stop();
    }

    public interface RunnableLoop {
        void start();

        void stop();
    }
}
