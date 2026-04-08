package org.jobgovernance.storage.postgres;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class PostgresExecutionRepositoryIT {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("jgk")
            .withUsername("jgk")
            .withPassword("jgk");

    @Test
    void postgresShouldStart() {
        assertTrue(postgres.isRunning());
    }
}
