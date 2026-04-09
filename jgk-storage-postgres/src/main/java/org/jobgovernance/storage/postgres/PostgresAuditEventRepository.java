package org.jobgovernance.storage.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jobgovernance.storage.spi.AuditEventRepository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class PostgresAuditEventRepository implements AuditEventRepository {

    private final DataSource dataSource;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public PostgresAuditEventRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void append(AuditEvent event) {
        String sql = """
                INSERT INTO job_audit_event(
                    event_id,
                    event_type,
                    job_key,
                    execution_id,
                    actor,
                    details_json,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?::jsonb, ?)
                """;
        UUID eventId = event.eventId() == null ? UUID.randomUUID() : event.eventId();
        Instant createdAt = event.createdAt() == null ? Instant.now() : event.createdAt();
        executeUpdate(sql, statement -> {
            statement.setObject(1, eventId);
            statement.setString(2, event.eventType());
            statement.setString(3, event.jobKey());
            statement.setObject(4, event.executionId());
            statement.setString(5, event.actor());
            statement.setString(6, sanitizeJson(event.detailsJson()));
            statement.setTimestamp(7, toTimestamp(createdAt));
        });
    }

    @Override
    public List<AuditEvent> findByJobKey(String jobKey, int limit) {
        String sql = """
                SELECT event_id,
                       event_type,
                       job_key,
                       execution_id,
                       actor,
                       details_json::text AS details_json,
                       created_at
                FROM job_audit_event
                WHERE job_key = ?
                ORDER BY created_at DESC
                LIMIT ?
                """;
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, jobKey);
            statement.setInt(2, Math.max(1, limit));
            return fetch(statement);
        } catch (SQLException exception) {
            throw repositoryException("find audit events by job key", exception);
        }
    }

    @Override
    public List<AuditEvent> findByExecutionId(UUID executionId, int limit) {
        String sql = """
                SELECT event_id,
                       event_type,
                       job_key,
                       execution_id,
                       actor,
                       details_json::text AS details_json,
                       created_at
                FROM job_audit_event
                WHERE execution_id = ?
                ORDER BY created_at DESC
                LIMIT ?
                """;
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setObject(1, executionId);
            statement.setInt(2, Math.max(1, limit));
            return fetch(statement);
        } catch (SQLException exception) {
            throw repositoryException("find audit events by execution id", exception);
        }
    }

    private List<AuditEvent> fetch(PreparedStatement statement) throws SQLException {
        List<AuditEvent> events = new ArrayList<>();
        try (ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                events.add(new AuditEvent(
                        resultSet.getObject("event_id", UUID.class),
                        resultSet.getString("event_type"),
                        resultSet.getString("job_key"),
                        resultSet.getObject("execution_id", UUID.class),
                        resultSet.getString("actor"),
                        resultSet.getString("details_json"),
                        toInstant(resultSet, "created_at")
                ));
            }
        }
        return events;
    }

    private void executeUpdate(String sql, StatementBinder binder) {
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            binder.bind(statement);
            statement.executeUpdate();
        } catch (SQLException exception) {
            throw repositoryException("append audit event", exception);
        }
    }

    private String sanitizeJson(String detailsJson) {
        String normalized = detailsJson == null || detailsJson.isBlank() ? "{}" : detailsJson;
        try {
            return objectMapper.readTree(normalized).toString();
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("detailsJson must be valid JSON", exception);
        }
    }

    private RuntimeException repositoryException(String action, SQLException exception) {
        return new IllegalStateException("PostgreSQL audit event repository failed to " + action, exception);
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
