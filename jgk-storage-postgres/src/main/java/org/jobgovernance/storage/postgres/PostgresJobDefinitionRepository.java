package org.jobgovernance.storage.postgres;

import org.jobgovernance.core.model.ExecutionMode;
import org.jobgovernance.core.model.JobDefinition;
import org.jobgovernance.core.model.JobDefinitionState;
import org.jobgovernance.core.model.JobSchedule;
import org.jobgovernance.storage.spi.JobDefinitionRepository;

import javax.sql.DataSource;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

public class PostgresJobDefinitionRepository implements JobDefinitionRepository {

    private final DataSource dataSource;
    private final PostgresPolicyJsonCodec policyCodec;

    public PostgresJobDefinitionRepository(DataSource dataSource) {
        this(dataSource, new PostgresPolicyJsonCodec());
    }

    PostgresJobDefinitionRepository(DataSource dataSource, PostgresPolicyJsonCodec policyCodec) {
        this.dataSource = dataSource;
        this.policyCodec = policyCodec;
    }

    @Override
    public void upsert(JobDefinition definition, Instant now) {
        String definitionSql = """
                INSERT INTO job_definition(
                    job_key,
                    version,
                    display_name,
                    description,
                    owner_team,
                    tags,
                    execution_mode,
                    payload_schema_version,
                    state,
                    manual_triggerable,
                    internal_only,
                    tenant_scope,
                    policy_json,
                    created_at,
                    updated_at,
                    updated_by
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?)
                ON CONFLICT (job_key) DO UPDATE
                SET version = EXCLUDED.version,
                    display_name = EXCLUDED.display_name,
                    description = EXCLUDED.description,
                    owner_team = EXCLUDED.owner_team,
                    tags = EXCLUDED.tags,
                    execution_mode = EXCLUDED.execution_mode,
                    payload_schema_version = EXCLUDED.payload_schema_version,
                    state = EXCLUDED.state,
                    manual_triggerable = EXCLUDED.manual_triggerable,
                    internal_only = EXCLUDED.internal_only,
                    tenant_scope = EXCLUDED.tenant_scope,
                    policy_json = EXCLUDED.policy_json,
                    updated_at = EXCLUDED.updated_at,
                    updated_by = EXCLUDED.updated_by
                """;
        String scheduleSql = """
                INSERT INTO job_schedule(
                    job_key,
                    schedule_kind,
                    cron_expression,
                    zone_id,
                    one_time_at,
                    fixed_delay_ms,
                    fixed_rate_ms,
                    initial_delay_ms,
                    effective_from,
                    effective_to,
                    misfire_policy,
                    enabled,
                    next_materialize_at,
                    last_evaluated_at,
                    cursor_version,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (job_key) DO UPDATE
                SET schedule_kind = EXCLUDED.schedule_kind,
                    cron_expression = EXCLUDED.cron_expression,
                    zone_id = EXCLUDED.zone_id,
                    one_time_at = EXCLUDED.one_time_at,
                    fixed_delay_ms = EXCLUDED.fixed_delay_ms,
                    fixed_rate_ms = EXCLUDED.fixed_rate_ms,
                    initial_delay_ms = EXCLUDED.initial_delay_ms,
                    effective_from = EXCLUDED.effective_from,
                    effective_to = EXCLUDED.effective_to,
                    misfire_policy = EXCLUDED.misfire_policy,
                    enabled = EXCLUDED.enabled,
                    updated_at = EXCLUDED.updated_at
                """;
        try (Connection connection = dataSource.getConnection()) {
            boolean previousAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                writeDefinition(connection, definitionSql, definition, now);
                writeSchedule(connection, scheduleSql, definition, now);
                connection.commit();
            } catch (SQLException exception) {
                connection.rollback();
                throw exception;
            } finally {
                connection.setAutoCommit(previousAutoCommit);
            }
        } catch (SQLException exception) {
            throw repositoryException("upsert job definition", exception);
        }
    }

    @Override
    public Optional<JobDefinition> findByJobKey(String jobKey) {
        String sql = """
                SELECT d.*,
                       s.schedule_kind,
                       s.cron_expression,
                       s.zone_id,
                       s.one_time_at,
                       s.fixed_delay_ms,
                       s.fixed_rate_ms,
                       s.initial_delay_ms,
                       s.effective_from,
                       s.effective_to
                FROM job_definition d
                LEFT JOIN job_schedule s ON d.job_key = s.job_key
                WHERE d.job_key = ?
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
                return Optional.of(mapDefinition(resultSet));
            }
        } catch (SQLException exception) {
            throw repositoryException("find by job key", exception);
        }
    }

    @Override
    public List<JobDefinition> findEnabled(int limit) {
        String sql = """
                SELECT d.*,
                       s.schedule_kind,
                       s.cron_expression,
                       s.zone_id,
                       s.one_time_at,
                       s.fixed_delay_ms,
                       s.fixed_rate_ms,
                       s.initial_delay_ms,
                       s.effective_from,
                       s.effective_to
                FROM job_definition d
                LEFT JOIN job_schedule s ON d.job_key = s.job_key
                WHERE d.state = 'ENABLED'
                  AND COALESCE(s.enabled, TRUE) = TRUE
                ORDER BY d.job_key
                LIMIT ?
                """;
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setInt(1, Math.max(1, limit));
            List<JobDefinition> result = new ArrayList<>(Math.max(1, limit));
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    result.add(mapDefinition(resultSet));
                }
            }
            return result;
        } catch (SQLException exception) {
            throw repositoryException("find enabled definitions", exception);
        }
    }

    @Override
    public boolean updateState(String jobKey, JobDefinitionState targetState, String actor, Instant changedAt) {
        String definitionSql = """
                UPDATE job_definition
                SET state = ?,
                    version = CASE WHEN state <> ? THEN version + 1 ELSE version END,
                    updated_by = ?,
                    updated_at = ?
                WHERE job_key = ?
                """;
        String scheduleSql = """
                UPDATE job_schedule
                SET enabled = ?,
                    updated_at = ?
                WHERE job_key = ?
                """;
        try (Connection connection = dataSource.getConnection()) {
            boolean previousAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try (
                    PreparedStatement definitionStatement = connection.prepareStatement(definitionSql);
                    PreparedStatement scheduleStatement = connection.prepareStatement(scheduleSql)
            ) {
                definitionStatement.setString(1, targetState.name());
                definitionStatement.setString(2, targetState.name());
                definitionStatement.setString(3, actor == null || actor.isBlank() ? "system" : actor);
                definitionStatement.setTimestamp(4, toTimestamp(changedAt));
                definitionStatement.setString(5, jobKey);
                int updatedDefinitions = definitionStatement.executeUpdate();
                if (updatedDefinitions < 1) {
                    connection.rollback();
                    return false;
                }

                scheduleStatement.setBoolean(1, targetState == JobDefinitionState.ENABLED);
                scheduleStatement.setTimestamp(2, toTimestamp(changedAt));
                scheduleStatement.setString(3, jobKey);
                scheduleStatement.executeUpdate();

                connection.commit();
                return true;
            } catch (SQLException exception) {
                connection.rollback();
                throw exception;
            } finally {
                connection.setAutoCommit(previousAutoCommit);
            }
        } catch (SQLException exception) {
            throw repositoryException("update definition state", exception);
        }
    }

    private void writeDefinition(Connection connection, String sql, JobDefinition definition, Instant now) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, definition.jobKey());
            statement.setInt(2, definition.version());
            statement.setString(3, definition.displayName());
            statement.setString(4, definition.description());
            statement.setString(5, definition.ownerTeam());
            statement.setArray(6, toTextArray(connection, definition.tags()));
            statement.setString(7, definition.executionMode().name());
            statement.setString(8, definition.payloadSchemaVersion());
            statement.setString(9, definition.state().name());
            statement.setBoolean(10, definition.manualTriggerable());
            statement.setBoolean(11, definition.internalOnly());
            statement.setString(12, definition.tenantScope());
            statement.setString(13, policyCodec.encode(definition.policy()));
            statement.setTimestamp(14, toTimestamp(now));
            statement.setTimestamp(15, toTimestamp(now));
            statement.setString(16, "registry-sync");
            statement.executeUpdate();
        }
    }

    private void writeSchedule(Connection connection, String sql, JobDefinition definition, Instant now) throws SQLException {
        ScheduleValues values = ScheduleValues.from(definition.schedule());
        boolean enabled = definition.state() == JobDefinitionState.ENABLED && definition.schedule().kind() != JobSchedule.Kind.DISABLED;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, definition.jobKey());
            statement.setString(2, definition.schedule().kind().name());
            statement.setString(3, values.cronExpression());
            statement.setString(4, values.zoneId());
            statement.setTimestamp(5, toTimestamp(values.oneTimeAt()));
            statement.setLong(6, values.fixedDelayMs());
            statement.setLong(7, values.fixedRateMs());
            statement.setLong(8, values.initialDelayMs());
            statement.setTimestamp(9, toTimestamp(values.effectiveFrom()));
            statement.setTimestamp(10, toTimestamp(values.effectiveTo()));
            statement.setString(11, definition.policy().misfirePolicy().name());
            statement.setBoolean(12, enabled);
            statement.setTimestamp(13, toTimestamp(values.nextMaterializeAt(now)));
            statement.setTimestamp(14, null);
            statement.setLong(15, 0L);
            statement.setTimestamp(16, toTimestamp(now));
            statement.executeUpdate();
        }
    }

    private JobDefinition mapDefinition(ResultSet resultSet) throws SQLException {
        return new JobDefinition(
                resultSet.getString("job_key"),
                resultSet.getInt("version"),
                resultSet.getString("display_name"),
                resultSet.getString("description"),
                resultSet.getString("owner_team"),
                toTags(resultSet.getArray("tags")),
                ExecutionMode.valueOf(resultSet.getString("execution_mode").toUpperCase(Locale.ROOT)),
                mapSchedule(resultSet),
                policyCodec.decode(resultSet.getString("policy_json")),
                resultSet.getString("payload_schema_version"),
                JobDefinitionState.valueOf(resultSet.getString("state").toUpperCase(Locale.ROOT)),
                resultSet.getBoolean("manual_triggerable"),
                resultSet.getBoolean("internal_only"),
                resultSet.getString("tenant_scope")
        );
    }

    private JobSchedule mapSchedule(ResultSet resultSet) throws SQLException {
        String scheduleKind = resultSet.getString("schedule_kind");
        ZoneId zoneId = ZoneId.of(resultSet.getString("zone_id") == null ? "UTC" : resultSet.getString("zone_id"));
        Instant effectiveFrom = toInstant(resultSet, "effective_from");
        Instant effectiveTo = toInstant(resultSet, "effective_to");
        if (scheduleKind == null) {
            return new JobSchedule.DisabledSchedule(zoneId, effectiveFrom, effectiveTo);
        }
        JobSchedule.Kind kind = JobSchedule.Kind.valueOf(scheduleKind.toUpperCase(Locale.ROOT));
        return switch (kind) {
            case ONE_TIME -> new JobSchedule.OneTimeSchedule(
                    requiredInstant(resultSet, "one_time_at"),
                    zoneId,
                    effectiveFrom,
                    effectiveTo
            );
            case FIXED_DELAY -> new JobSchedule.FixedDelaySchedule(
                    Duration.ofMillis(requiredLong(resultSet, "fixed_delay_ms", 1L)),
                    Duration.ofMillis(optionalLong(resultSet, "initial_delay_ms", 0L)),
                    zoneId,
                    effectiveFrom,
                    effectiveTo
            );
            case FIXED_RATE -> new JobSchedule.FixedRateSchedule(
                    Duration.ofMillis(requiredLong(resultSet, "fixed_rate_ms", 1L)),
                    Duration.ofMillis(optionalLong(resultSet, "initial_delay_ms", 0L)),
                    zoneId,
                    effectiveFrom,
                    effectiveTo
            );
            case CRON -> new JobSchedule.CronSchedule(
                    resultSet.getString("cron_expression"),
                    zoneId,
                    effectiveFrom,
                    effectiveTo
            );
            case DISABLED -> new JobSchedule.DisabledSchedule(zoneId, effectiveFrom, effectiveTo);
        };
    }

    private RuntimeException repositoryException(String action, SQLException exception) {
        return new IllegalStateException("PostgreSQL job definition repository failed to " + action, exception);
    }

    private static Array toTextArray(Connection connection, Set<String> tags) throws SQLException {
        return connection.createArrayOf("text", tags == null ? new String[0] : tags.stream().sorted().toArray(String[]::new));
    }

    private static Set<String> toTags(Array array) throws SQLException {
        if (array == null) {
            return Set.of();
        }
        Object value = array.getArray();
        if (!(value instanceof String[] tags)) {
            return Set.of();
        }
        Set<String> result = new LinkedHashSet<>();
        for (String tag : tags) {
            if (tag != null && !tag.isBlank()) {
                result.add(tag);
            }
        }
        return Set.copyOf(result);
    }

    private static long requiredLong(ResultSet resultSet, String column, long fallback) throws SQLException {
        long value = resultSet.getLong(column);
        if (resultSet.wasNull() || value < 1) {
            return fallback;
        }
        return value;
    }

    private static long optionalLong(ResultSet resultSet, String column, long fallback) throws SQLException {
        long value = resultSet.getLong(column);
        return resultSet.wasNull() ? fallback : value;
    }

    private static Instant requiredInstant(ResultSet resultSet, String column) throws SQLException {
        Instant value = toInstant(resultSet, column);
        if (value == null) {
            throw new IllegalStateException("Expected non-null instant in column " + column);
        }
        return value;
    }

    private static Timestamp toTimestamp(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }

    private static Instant toInstant(ResultSet resultSet, String columnName) throws SQLException {
        Timestamp timestamp = resultSet.getTimestamp(columnName);
        return timestamp == null ? null : timestamp.toInstant();
    }

    private record ScheduleValues(
            String cronExpression,
            String zoneId,
            Instant oneTimeAt,
            long fixedDelayMs,
            long fixedRateMs,
            long initialDelayMs,
            Instant effectiveFrom,
            Instant effectiveTo
    ) {
        static ScheduleValues from(JobSchedule schedule) {
            return switch (schedule) {
                case JobSchedule.OneTimeSchedule oneTime -> new ScheduleValues(
                        null,
                        oneTime.zoneId().getId(),
                        oneTime.scheduledAt(),
                        0L,
                        0L,
                        0L,
                        oneTime.effectiveFrom(),
                        oneTime.effectiveTo()
                );
                case JobSchedule.FixedDelaySchedule fixedDelay -> new ScheduleValues(
                        null,
                        fixedDelay.zoneId().getId(),
                        null,
                        fixedDelay.delay().toMillis(),
                        0L,
                        fixedDelay.initialDelay().toMillis(),
                        fixedDelay.effectiveFrom(),
                        fixedDelay.effectiveTo()
                );
                case JobSchedule.FixedRateSchedule fixedRate -> new ScheduleValues(
                        null,
                        fixedRate.zoneId().getId(),
                        null,
                        0L,
                        fixedRate.rate().toMillis(),
                        fixedRate.initialDelay().toMillis(),
                        fixedRate.effectiveFrom(),
                        fixedRate.effectiveTo()
                );
                case JobSchedule.CronSchedule cron -> new ScheduleValues(
                        cron.cronExpression(),
                        cron.zoneId().getId(),
                        null,
                        0L,
                        0L,
                        0L,
                        cron.effectiveFrom(),
                        cron.effectiveTo()
                );
                case JobSchedule.DisabledSchedule disabled -> new ScheduleValues(
                        null,
                        disabled.zoneId().getId(),
                        null,
                        0L,
                        0L,
                        0L,
                        disabled.effectiveFrom(),
                        disabled.effectiveTo()
                );
            };
        }

        Instant nextMaterializeAt(Instant now) {
            if (oneTimeAt != null) {
                return oneTimeAt;
            }
            if (fixedDelayMs > 0 || fixedRateMs > 0) {
                return now.plusMillis(Math.max(0L, initialDelayMs));
            }
            return null;
        }
    }
}
