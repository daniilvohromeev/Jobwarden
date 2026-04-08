package org.jobgovernance.storage.spi.tck;

import org.jobgovernance.storage.spi.ExecutionRepository;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public interface ExecutionRepositoryTck {

    ExecutionRepository createRepository();

    @Test
    default void repositoryShouldExist() {
        assertNotNull(createRepository());
    }

    @Test
    default void dueFetchShouldNotFailForEmptyStore() {
        createRepository().findDueExecutionIds(Instant.now(), 10);
    }
}
