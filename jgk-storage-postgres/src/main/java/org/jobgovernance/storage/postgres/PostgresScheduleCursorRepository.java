package org.jobgovernance.storage.postgres;

import org.jobgovernance.storage.spi.ScheduleCursorRepository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;

public class PostgresScheduleCursorRepository implements ScheduleCursorRepository {

    private final DataSource dataSource;

    public PostgresScheduleCursorRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Optional<ScheduleCursor> findByJobKey(String jobKey) {
        String sql = """
                SELECT job_key, last_evaluated_at, next_materialize_at, cursor_version
                FROM job_schedule
                WHERE job_key = ?
                """;
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, jobKey);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }
                return Optional.of(new ScheduleCursor(
                        resultSet.getString("job_key"),
                        toInstant(resultSet, "last_evaluated_at"),
                        toInstant(resultSet, "next_materialize_at"),
                        resultSet.getLong("cursor_version")
                ));
            }
        } catch (SQLException exception) {
            throw repositoryException("find schedule cursor", exception);
        }
    }

    @Override
    public void updateCursor(ScheduleCursor cursor, Instant updatedAt) {
        String sql = """
                UPDATE job_schedule
                SET last_evaluated_at = ?,
                    next_materialize_at = ?,
                    cursor_version = cursor_version + 1,
                    updated_at = ?
                WHERE job_key = ?
                  AND cursor_version = ?
                """;
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setTimestamp(1, toTimestamp(cursor.lastEvaluatedAt()));
            statement.setTimestamp(2, toTimestamp(cursor.nextMaterializeAt()));
            statement.setTimestamp(3, toTimestamp(updatedAt));
            statement.setString(4, cursor.jobKey());
            statement.setLong(5, cursor.cursorVersion());
            int updated = statement.executeUpdate();
            if (updated < 1) {
                throw new IllegalStateException("Failed to update schedule cursor for jobKey=" + cursor.jobKey());
            }
        } catch (SQLException exception) {
            throw repositoryException("update schedule cursor", exception);
        }
    }

    private RuntimeException repositoryException(String action, SQLException exception) {
        return new IllegalStateException("PostgreSQL schedule cursor repository failed to " + action, exception);
    }

    private static Timestamp toTimestamp(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }

    private static Instant toInstant(ResultSet resultSet, String columnName) throws SQLException {
        Timestamp timestamp = resultSet.getTimestamp(columnName);
        return timestamp == null ? null : timestamp.toInstant();
    }
}
