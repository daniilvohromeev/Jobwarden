package org.jobgovernance.spring.core;

import org.jobgovernance.core.api.JobRegistry;
import org.jobgovernance.core.model.JobDefinition;
import org.jobgovernance.core.model.JobDefinitionState;
import org.jobgovernance.core.model.JobExecution;
import org.jobgovernance.core.model.TriggerType;
import org.jobgovernance.storage.spi.AuditEventRepository;
import org.jobgovernance.storage.spi.ExecutionRepository;
import org.jobgovernance.storage.spi.JobDefinitionRepository;
import org.jobgovernance.storage.spi.ManualTriggerRequestRepository;

import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public final class DefaultJobGovernanceManagementService implements JobGovernanceManagementService {

    private static final int DEFAULT_MAX_ATTEMPTS = 3;

    private final JobRegistry jobRegistry;
    private final ExecutionRepository executionRepository;
    private final JobDefinitionRepository jobDefinitionRepository;
    private final ManualTriggerRequestRepository triggerRequestRepository;
    private final AuditEventRepository auditEventRepository;
    private final Clock clock;

    public DefaultJobGovernanceManagementService(
            JobRegistry jobRegistry,
            ExecutionRepository executionRepository
    ) {
        this(jobRegistry, executionRepository, null, null, null, Clock.systemUTC());
    }

    DefaultJobGovernanceManagementService(
            JobRegistry jobRegistry,
            ExecutionRepository executionRepository,
            Clock clock
    ) {
        this(jobRegistry, executionRepository, null, null, null, clock);
    }

    DefaultJobGovernanceManagementService(
            JobRegistry jobRegistry,
            ExecutionRepository executionRepository,
            JobDefinitionRepository jobDefinitionRepository,
            ManualTriggerRequestRepository triggerRequestRepository,
            AuditEventRepository auditEventRepository,
            Clock clock
    ) {
        this.jobRegistry = Objects.requireNonNull(jobRegistry, "jobRegistry is required");
        this.executionRepository = Objects.requireNonNull(executionRepository, "executionRepository is required");
        this.jobDefinitionRepository = jobDefinitionRepository;
        this.triggerRequestRepository = triggerRequestRepository;
        this.auditEventRepository = auditEventRepository;
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
        return updateDefinitionState(jobKey, JobDefinitionState.PAUSED, actor);
    }

    @Override
    public boolean resumeJob(String jobKey, String actor) {
        return updateDefinitionState(jobKey, JobDefinitionState.ENABLED, actor);
    }

    @Override
    public boolean cancelExecution(UUID executionId, String actor, String reason) {
        boolean cancelled = executionRepository.requestCancellation(
                requireExecutionId(executionId),
                sanitizeActor(actor),
                sanitizeReason(reason),
                clock.instant()
        );
        if (cancelled) {
            appendAudit(
                    "EXECUTION_CANCEL_REQUESTED",
                    null,
                    executionId,
                    actor,
                    Map.of("reason", sanitizeReason(reason))
            );
        }
        return cancelled;
    }

    @Override
    public List<ExecutionView> listExecutions(String jobKey, String tenantId, int limit) {
        String normalizedJobKey = requireJobKey(jobKey);
        int cappedLimit = capLimit(limit);
        return executionRepository.findExecutions(normalizedJobKey, emptyToNull(tenantId), cappedLimit).stream()
                .map(this::toExecutionView)
                .toList();
    }

    @Override
    public List<AuditView> listAuditEventsByJob(String jobKey, int limit) {
        if (auditEventRepository == null) {
            return List.of();
        }
        String normalizedJobKey = requireJobKey(jobKey);
        int cappedLimit = capLimit(limit);
        return auditEventRepository.findByJobKey(normalizedJobKey, cappedLimit).stream()
                .map(this::toAuditView)
                .toList();
    }

    @Override
    public List<AuditView> listAuditEventsByExecution(UUID executionId, int limit) {
        if (auditEventRepository == null) {
            return List.of();
        }
        UUID normalizedExecutionId = requireExecutionId(executionId);
        int cappedLimit = capLimit(limit);
        return auditEventRepository.findByExecutionId(normalizedExecutionId, cappedLimit).stream()
                .map(this::toAuditView)
                .toList();
    }

    @Override
    public List<WorkerView> listActiveWorkers(int limit) {
        int cappedLimit = capLimit(limit);
        return executionRepository.findActiveWorkers(clock.instant(), cappedLimit).stream()
                .map(worker -> new WorkerView(
                        worker.workerId(),
                        worker.activeExecutions(),
                        worker.oldestClaimedAt(),
                        worker.lastHeartbeatAt(),
                        worker.leaseExpiresAt()
                ))
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
                appendAudit(
                        "MANUAL_TRIGGER_DEDUPLICATED",
                        normalizedJobKey,
                        existing.get().executionId(),
                        normalizedActor,
                        Map.of(
                                "tenantId", normalizedTenant == null ? "" : normalizedTenant,
                                "idempotencyKey", normalizedIdempotencyKey
                        )
                );
                return toExecutionView(existing.get());
            }
        }

        int maxAttempts = registration.definition().policy().retryStrategy()
                .maxAttemptsHint()
                .orElse(DEFAULT_MAX_ATTEMPTS);
        Instant createdAt = clock.instant();
        String payloadRef = payloadRef(payloadJson);
        String dedupeKey = dedupeKey(normalizedJobKey, normalizedTenant, scheduledAt, normalizedIdempotencyKey);
        UUID requestId = null;
        if (triggerRequestRepository != null) {
            requestId = triggerRequestRepository.enqueue(new ManualTriggerRequestRepository.ManualTriggerRequest(
                    UUID.randomUUID(),
                    normalizedJobKey,
                    normalizedTenant,
                    payloadRef,
                    normalizedIdempotencyKey,
                    scheduledAt,
                    normalizedActor,
                    createdAt
            ));
        }

        ExecutionRepository.ScheduledExecutionInsert insert = new ExecutionRepository.ScheduledExecutionInsert(
                normalizedJobKey,
                normalizedTenant,
                TriggerType.MANUAL.name(),
                scheduledAt,
                scheduledAt,
                maxAttempts,
                payloadRef,
                dedupeKey,
                normalizedActor + "|" + createdAt,
                null,
                null,
                null,
                normalizedIdempotencyKey,
                null
        );
        executionRepository.enqueueScheduledExecution(insert, createdAt);

        ExecutionView view = resolveExecutionView(normalizedJobKey, normalizedTenant, scheduledAt, normalizedIdempotencyKey);
        if (requestId != null) {
            triggerRequestRepository.markProcessed(requestId, createdAt, view.executionId());
        }
        appendAudit(
                "MANUAL_TRIGGER_ENQUEUED",
                normalizedJobKey,
                view.executionId(),
                normalizedActor,
                Map.of(
                        "tenantId", normalizedTenant == null ? "" : normalizedTenant,
                        "scheduledAt", scheduledAt.toString(),
                        "idempotencyKey", normalizedIdempotencyKey == null ? "" : normalizedIdempotencyKey
                )
        );
        return view;
    }

    private ExecutionView resolveExecutionView(
            String jobKey,
            String tenantId,
            Instant scheduledAt,
            String idempotencyKey
    ) {
        if (idempotencyKey != null) {
            return executionRepository.findByIdempotencyKey(jobKey, tenantId, idempotencyKey)
                    .map(this::toExecutionView)
                    .orElseThrow(() -> new IllegalStateException("Unable to load execution after trigger enqueue"));
        }
        return executionRepository.findExecutions(jobKey, tenantId, 5).stream()
                .filter(execution -> execution.scheduledAt().equals(scheduledAt))
                .findFirst()
                .map(this::toExecutionView)
                .orElseThrow(() -> new IllegalStateException("Unable to resolve execution after trigger enqueue"));
    }

    private boolean updateDefinitionState(String jobKey, JobDefinitionState targetState, String actor) {
        String normalizedJobKey = requireJobKey(jobKey);
        JobRegistry.JobRegistration<?, ?> registration = jobRegistry.findByJobKey(normalizedJobKey).orElse(null);
        Instant now = clock.instant();
        String normalizedActor = sanitizeActor(actor);

        boolean storageUpdated = false;
        if (jobDefinitionRepository != null && registration != null) {
            jobDefinitionRepository.upsert(registration.definition(), now);
        }
        if (jobDefinitionRepository != null) {
            storageUpdated = jobDefinitionRepository.updateState(normalizedJobKey, targetState, normalizedActor, now);
        }

        if (registration == null) {
            return storageUpdated;
        }
        boolean registryUpdated = updateRegistryState(registration, targetState);
        if (registryUpdated) {
            appendAudit(
                    targetState == JobDefinitionState.ENABLED ? "JOB_RESUMED" : "JOB_PAUSED",
                    normalizedJobKey,
                    null,
                    normalizedActor,
                    Map.of("state", targetState.name())
            );
        }
        return storageUpdated || registryUpdated;
    }

    private boolean updateRegistryState(JobRegistry.JobRegistration<?, ?> registration, JobDefinitionState targetState) {
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

    private AuditView toAuditView(AuditEventRepository.AuditEvent event) {
        return new AuditView(
                event.eventId(),
                event.eventType(),
                event.jobKey(),
                event.executionId(),
                event.actor(),
                event.detailsJson(),
                event.createdAt()
        );
    }

    private void appendAudit(
            String eventType,
            String jobKey,
            UUID executionId,
            String actor,
            Map<String, String> details
    ) {
        if (auditEventRepository == null) {
            return;
        }
        Map<String, String> normalized = new LinkedHashMap<>();
        if (details != null) {
            normalized.putAll(details);
        }
        auditEventRepository.append(new AuditEventRepository.AuditEvent(
                UUID.randomUUID(),
                eventType,
                jobKey,
                executionId,
                sanitizeActor(actor),
                toJson(normalized),
                clock.instant()
        ));
    }

    private static String toJson(Map<String, String> details) {
        if (details == null || details.isEmpty()) {
            return "{}";
        }
        StringBuilder builder = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> entry : details.entrySet()) {
            if (!first) {
                builder.append(',');
            }
            builder.append('"').append(escapeJson(entry.getKey())).append('"')
                    .append(':')
                    .append('"').append(escapeJson(entry.getValue())).append('"');
            first = false;
        }
        builder.append('}');
        return builder.toString();
    }

    private static String escapeJson(String value) {
        if (value == null) {
            return "";
        }
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"");
    }
}
