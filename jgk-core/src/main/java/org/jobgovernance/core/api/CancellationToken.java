package org.jobgovernance.core.api;

import java.util.concurrent.CancellationException;

public interface CancellationToken {

    boolean isCancellationRequested();

    default void throwIfCancellationRequested() {
        if (isCancellationRequested()) {
            throw new CancellationException("Cancellation requested");
        }
    }

    static CancellationToken none() {
        return () -> false;
    }
}
