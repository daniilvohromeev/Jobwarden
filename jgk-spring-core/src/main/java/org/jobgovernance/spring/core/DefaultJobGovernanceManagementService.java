package org.jobgovernance.spring.core;

import org.jobgovernance.core.api.JobRegistry;
import org.jobgovernance.core.model.JobDefinition;
import org.jobgovernance.core.model.JobDefinitionState;
import org.jobgovernance.core.model.JobExecution;
import org.jobgovernance.core.model.TriggerType;
import org.jobgovernance.storage.spi.ExecutionRepository;

import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public final class DefaultJobGovernanceManagementService implements JobGovernanceManagementService {

    private static final int DEFAULT_MAX_ATTEMPTS = 3;

    private final JobRegistry jobRegistry;
    private final ExecutionRepository executionRepository;
    private final Clock clock;

    public DefaultJobGovernanceManagementService(
            JobRegistry jobRegistry,
            ExecutionRepository executionRepository
    ) {
        this(jobRegistry, executionRepository, Clock.systemUTC());
    }

    DefaultJobGovernanceManagementService(
            JobRegistry jobRegistry,
            ExecutionRepository executionRepository,
            Clock clock
    ) {
        this.jobRegistry = Objects.requireNonNull(jobRegistry, "jobRegistry is required");
        this.executionRepository = Objects.requireNonNull(executionRepository, "executionRepository is required");
        this.clock = Objects.requireNonNull(clock, "clock is required");
    }

    @Override
    public List<JobView> listJobs(String tenantId, int limit) {
        int cappedLimit = capLimit(limit);
        return jobRegistry.all().stream()
                .map(JobRegistry.JobRegistration::definition)
                .filter(definition -> includesTenant(definition, tenantId))
                .sorted(Comparator.comparing(JobDefinition::jobKey))
                .limit(cappedLimit)
                .map(definition -> new JobView(
                        definition.jobKey(),
                        definition.displayName(),
                        definition.state().name(),
                        definition.ownerTeam(),
                        definition.tags().stream().sorted().toList()
                ))
                .toList();
    }

    @Override
    public ExecutionView triggerNow(String jobKey, String tenantId, String actor, String payloadJson, String idempotencyKey) {
        return trigger(jobKey, tenantId, actor, payloadJson, clock.instant(), idempotencyKey);
    }

    @Override
    public ExecutionView triggerAt(String jobKey, String tenantId, String actor, String payloadJson, Instant triggerAt, String idempotencyKey) {
        if (triggerAt == null) {
            throw new IllegalArgumentException("triggerAt is required");
        }
        return trigger(jobKey, tenantId, actor, payloadJson, triggerAt, idempotencyKey);
    }

    @Override
    public boolean pauseJob(String jobKey, String actor) {
        return updateDefinitionState(jobKey, JobDefinitionState.PAUSED);
    }

    @Override
    public boolean resumeJob(String jobKey, String actor) {
        return updateDefinitionState(jobKey, JobDefinitionState.ENABLED);
    }

    @Override
    public boolean cancelExecution(UUID executionId, String actor, String reason) {
        return executionRepository.requestCancellation(
                requireExecutionId(executionId),
                sanitizeActor(actor),
                sanitizeReason(reason),
                clock.instant()
        );
    }

    @Override
    public List<ExecutionView> listExecutions(String jobKey, String tenantId, int limit) {
        String normalizedJobKey = requireJobKey(jobKey);
        int cappedLimit = capLimit(limit);
        return executionRepository.findExecutions(normalizedJobKey, emptyToNull(tenantId), cappedLimit).stream()
                .map(this::toExecutionView)
                .toList();
    }

    private ExecutionView trigger(
            String jobKey,
            String tenantId,
            String actor,
            String payloadJson,
            Instant scheduledAt,
            String idempotencyKey
    ) {
        String normalizedJobKey = requireJobKey(jobKey);
        String normalizedTenant = emptyToNull(tenantId);
        String normalizedActor = sanitizeActor(actor);
        String normalizedIdempotencyKey = emptyToNull(idempotencyKey);

        JobRegistry.JobRegistration<?, ?> registration = jobRegistry.findByJobKey(normalizedJobKey)
                .orElseThrow(() -> new IllegalArgumentException("Unknown jobKey=" + normalizedJobKey));
        if (!registration.definition().manualTriggerable()) {
            throw new IllegalStateException("Job is not manual-triggerable jobKey=" + normalizedJobKey);
        }

        if (normalizedIdempotencyKey != null) {
            Optional<JobExecution> existing = executionRepository.findByIdempotencyKey(
                    normalizedJobKey,
                    normalizedTenant,
                    normalizedIdempotencyKey
            );
            if (existing.isPresent()) {
                return toExecutionView(existing.get());
            }
        }

        int maxAttempts = registration.definition().policy().retryStrategy()
                .maxAttemptsHint()
                .orElse(DEFAULT_MAX_ATTEMPTS);
        Instant createdAt = clock.instant();
        String dedupeKey = dedupeKey(normalizedJobKey, normalizedTenant, scheduledAt, normalizedIdempotencyKey);
        ExecutionRepository.ScheduledExecutionInsert insert = new ExecutionRepository.ScheduledExecutionInsert(
                normalizedJobKey,
                normalizedTenant,
                TriggerType.MANUAL.name(),
                scheduledAt,
                scheduledAt,
                maxAttempts,
                payloadRef(payloadJson),
                dedupeKey,
                normalizedActor + "|" + createdAt,
                null,
                null,
                null,
                normalizedIdempotencyKey,
                null
        );
        executionRepository.enqueueScheduledExecution(insert, createdAt);

        if (normalizedIdempotencyKey != null) {
            return executionRepository.findByIdempotencyKey(normalizedJobKey, normalizedTenant, normalizedIdempotencyKey)
                    .map(this::toExecutionView)
                    .orElseThrow(() -> new IllegalStateException("Unable to load execution after trigger enqueue"));
        }

        return executionRepository.findExecutions(normalizedJobKey, normalizedTenant, 5).stream()
                .filter(execution -> execution.scheduledAt().equals(scheduledAt))
                .findFirst()
                .map(this::toExecutionView)
                .orElseThrow(() -> new IllegalStateException("Unable to resolve execution after trigger enqueue"));
    }

    private boolean updateDefinitionState(String jobKey, JobDefinitionState targetState) {
        String normalizedJobKey = requireJobKey(jobKey);
        JobRegistry.JobRegistration<?, ?> registration = jobRegistry.findByJobKey(normalizedJobKey).orElse(null);
        if (registration == null) {
            return false;
        }
        JobDefinition current = registration.definition();
        if (current.state() == targetState) {
            return true;
        }
        JobDefinition updated = new JobDefinition(
                current.jobKey(),
                current.version() + 1,
                current.displayName(),
                current.description(),
                current.ownerTeam(),
                current.tags(),
                current.executionMode(),
                current.schedule(),
                current.policy(),
                current.payloadSchemaVersion(),
                targetState,
                current.manualTriggerable(),
                current.internalOnly(),
                current.tenantScope()
        );
        @SuppressWarnings("unchecked")
        JobRegistry.JobRegistration<Object, Object> typed = (JobRegistry.JobRegistration<Object, Object>) registration;
        jobRegistry.register(new JobRegistry.JobRegistration<>(
                updated,
                typed.payloadType(),
                typed.handlerType(),
                typed.syncHandler(),
                typed.asyncHandler()
        ));
        return true;
    }

    private ExecutionView toExecutionView(JobExecution execution) {
        return new ExecutionView(
                execution.executionId(),
                execution.jobKey(),
                execution.status().name(),
                execution.scheduledAt(),
                execution.startedAt(),
                execution.finishedAt(),
                execution.attempt(),
                execution.workerId(),
                Map.of(
                        "self", "/jgk/v1/executions/" + execution.executionId(),
                        "cancel", "/jgk/v1/executions/" + execution.executionId() + "/cancel"
                )
        );
    }

    private static int capLimit(int limit) {
        if (limit <= 0) {
            return 100;
        }
        return Math.min(limit, 1_000);
    }

    private static boolean includesTenant(JobDefinition definition, String tenantId) {
        if (tenantId == null || tenantId.isBlank()) {
            return true;
        }
        return definition.tenantScope() == null || definition.tenantScope().equals(tenantId);
    }

    private static UUID requireExecutionId(UUID executionId) {
        if (executionId == null) {
            throw new IllegalArgumentException("executionId is required");
        }
        return executionId;
    }

    private static String requireJobKey(String jobKey) {
        if (jobKey == null || jobKey.isBlank()) {
            throw new IllegalArgumentException("jobKey is required");
        }
        return jobKey;
    }

    private static String sanitizeActor(String actor) {
        return actor == null || actor.isBlank() ? "system" : actor;
    }

    private static String sanitizeReason(String reason) {
        return reason == null || reason.isBlank() ? "manual cancellation" : reason;
    }

    private static String dedupeKey(
            String jobKey,
            String tenantId,
            Instant scheduledAt,
            String idempotencyKey
    ) {
        if (idempotencyKey != null) {
            return "manual|" + jobKey + "|" + (tenantId == null ? "" : tenantId) + "|" + idempotencyKey;
        }
        return "manual|" + jobKey + "|" + scheduledAt.toEpochMilli() + "|" + UUID.randomUUID();
    }

    private static String payloadRef(String payloadJson) {
        if (payloadJson == null || payloadJson.isBlank()) {
            return null;
        }
        String compact = payloadJson.length() > 2_000 ? payloadJson.substring(0, 2_000) : payloadJson;
        return "inline-json:" + compact;
    }

    private static String emptyToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
