package org.jobgovernance.storage.postgres;

import org.jobgovernance.storage.spi.ManualTriggerRequestRepository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class PostgresManualTriggerRequestRepository implements ManualTriggerRequestRepository {

    private final DataSource dataSource;

    public PostgresManualTriggerRequestRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public UUID enqueue(ManualTriggerRequest request) {
        UUID requestId = request.requestId() == null ? UUID.randomUUID() : request.requestId();
        String sql = """
                INSERT INTO job_trigger_request(
                    request_id,
                    job_key,
                    tenant_id,
                    payload_ref,
                    idempotency_key,
                    trigger_at,
                    actor,
                    requested_at,
                    state
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')
                ON CONFLICT DO NOTHING
                """;
        Instant requestedAt = request.requestedAt() == null ? Instant.now() : request.requestedAt();
        int inserted = executeUpdate(sql, statement -> {
            statement.setObject(1, requestId);
            statement.setString(2, request.jobKey());
            statement.setString(3, request.tenantId());
            statement.setString(4, request.payloadRef());
            statement.setString(5, request.idempotencyKey());
            statement.setTimestamp(6, toTimestamp(request.triggerAt()));
            statement.setString(7, request.actor());
            statement.setTimestamp(8, toTimestamp(requestedAt));
        });
        if (inserted > 0) {
            return requestId;
        }
        if (request.idempotencyKey() == null || request.idempotencyKey().isBlank()) {
            throw new IllegalStateException("Manual trigger request was not inserted and no idempotency key was provided");
        }
        return findByIdempotencyKey(request.jobKey(), request.tenantId(), request.idempotencyKey())
                .orElseThrow(() -> new IllegalStateException("Manual trigger dedupe failed to resolve existing request id"));
    }

    @Override
    public List<ManualTriggerRequest> fetchPending(int batchSize, Instant now) {
        String sql = """
                SELECT request_id,
                       job_key,
                       tenant_id,
                       payload_ref,
                       idempotency_key,
                       trigger_at,
                       actor,
                       requested_at
                FROM job_trigger_request
                WHERE state = 'PENDING'
                  AND trigger_at <= ?
                ORDER BY trigger_at, request_id
                LIMIT ?
                """;
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setTimestamp(1, toTimestamp(now));
            statement.setInt(2, Math.max(1, batchSize));
            List<ManualTriggerRequest> requests = new ArrayList<>(Math.max(1, batchSize));
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    requests.add(new ManualTriggerRequest(
                            resultSet.getObject("request_id", UUID.class),
                            resultSet.getString("job_key"),
                            resultSet.getString("tenant_id"),
                            resultSet.getString("payload_ref"),
                            resultSet.getString("idempotency_key"),
                            toInstant(resultSet, "trigger_at"),
                            resultSet.getString("actor"),
                            toInstant(resultSet, "requested_at")
                    ));
                }
            }
            return requests;
        } catch (SQLException exception) {
            throw repositoryException("fetch pending manual triggers", exception);
        }
    }

    @Override
    public boolean markProcessed(UUID requestId, Instant processedAt, UUID executionId) {
        String sql = """
                UPDATE job_trigger_request
                SET state = 'PROCESSED',
                    processed_at = ?,
                    execution_id = ?
                WHERE request_id = ?
                  AND state = 'PENDING'
                """;
        return executeUpdate(sql, statement -> {
            statement.setTimestamp(1, toTimestamp(processedAt));
            statement.setObject(2, executionId);
            statement.setObject(3, requestId);
        }) > 0;
    }

    private Optional<UUID> findByIdempotencyKey(String jobKey, String tenantId, String idempotencyKey) {
        String sql = """
                SELECT request_id
                FROM job_trigger_request
                WHERE job_key = ?
                  AND idempotency_key = ?
                  AND ((? IS NULL AND tenant_id IS NULL) OR tenant_id = ?)
                ORDER BY requested_at DESC
                LIMIT 1
                """;
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, jobKey);
            statement.setString(2, idempotencyKey);
            statement.setString(3, tenantId);
            statement.setString(4, tenantId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }
                return Optional.of(resultSet.getObject(1, UUID.class));
            }
        } catch (SQLException exception) {
            throw repositoryException("find manual trigger by idempotency key", exception);
        }
    }

    private int executeUpdate(String sql, StatementBinder binder) {
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            binder.bind(statement);
            return statement.executeUpdate();
        } catch (SQLException exception) {
            throw repositoryException("execute update", exception);
        }
    }

    private RuntimeException repositoryException(String action, SQLException exception) {
        return new IllegalStateException("PostgreSQL manual trigger repository failed to " + action, exception);
    }

    private static Timestamp toTimestamp(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }

    private static Instant toInstant(ResultSet resultSet, String columnName) throws SQLException {
        Timestamp timestamp = resultSet.getTimestamp(columnName);
        return timestamp == null ? null : timestamp.toInstant();
    }

    @FunctionalInterface
    private interface StatementBinder {
        void bind(PreparedStatement statement) throws SQLException;
    }
}
