package org.jobgovernance.spring.core;

import org.jobgovernance.core.api.ClaimService;
import org.jobgovernance.core.api.InMemoryJobRegistry;
import org.jobgovernance.core.api.JobRegistry;
import org.jobgovernance.core.api.ScheduleEvaluator;
import org.jobgovernance.core.model.JobExecution;
import org.jobgovernance.core.schedule.DefaultScheduleEvaluator;
import org.jobgovernance.executor.ActiveExecutionTracker;
import org.jobgovernance.executor.ExecutionEngine;
import org.jobgovernance.executor.HandlerRunnerLoop;
import org.jobgovernance.executor.HeartbeatLoop;
import org.jobgovernance.executor.InMemoryActiveExecutionTracker;
import org.jobgovernance.executor.PollingClaimLoop;
import org.jobgovernance.executor.RecoveryLoop;
import org.jobgovernance.executor.RepositoryClaimService;
import org.jobgovernance.executor.RetryRequeueLoop;
import org.jobgovernance.executor.SchedulerMaterializationLoop;
import org.jobgovernance.storage.spi.ExecutionRepository;
import org.jobgovernance.storage.spi.JobDefinitionRepository;
import org.jobgovernance.storage.spi.ManualTriggerRequestRepository;
import org.jobgovernance.storage.spi.ScheduleCursorRepository;
import org.jobgovernance.storage.spi.AuditEventRepository;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.sql.DataSource;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@AutoConfiguration
@EnableConfigurationProperties(JgkProperties.class)
@ConditionalOnProperty(prefix = "jgk", name = "enabled", havingValue = "true", matchIfMissing = true)
public class JobGovernanceAutoConfiguration {

    @Bean
    @ConditionalOnBean(DataSource.class)
    @ConditionalOnClass(name = "org.jobgovernance.storage.postgres.PostgresExecutionRepository")
    @ConditionalOnMissingBean
    public ExecutionRepository jgkExecutionRepository(DataSource dataSource) {
        return instantiatePostgresRepository(
                "org.jobgovernance.storage.postgres.PostgresExecutionRepository",
                dataSource,
                ExecutionRepository.class
        );
    }

    @Bean
    @ConditionalOnBean(DataSource.class)
    @ConditionalOnClass(name = "org.jobgovernance.storage.postgres.PostgresJobDefinitionRepository")
    @ConditionalOnMissingBean
    public JobDefinitionRepository jgkJobDefinitionRepository(DataSource dataSource) {
        return instantiatePostgresRepository(
                "org.jobgovernance.storage.postgres.PostgresJobDefinitionRepository",
                dataSource,
                JobDefinitionRepository.class
        );
    }

    @Bean
    @ConditionalOnBean(DataSource.class)
    @ConditionalOnClass(name = "org.jobgovernance.storage.postgres.PostgresScheduleCursorRepository")
    @ConditionalOnMissingBean
    public ScheduleCursorRepository jgkScheduleCursorRepository(DataSource dataSource) {
        return instantiatePostgresRepository(
                "org.jobgovernance.storage.postgres.PostgresScheduleCursorRepository",
                dataSource,
                ScheduleCursorRepository.class
        );
    }

    @Bean
    @ConditionalOnBean(DataSource.class)
    @ConditionalOnClass(name = "org.jobgovernance.storage.postgres.PostgresManualTriggerRequestRepository")
    @ConditionalOnMissingBean
    public ManualTriggerRequestRepository jgkManualTriggerRequestRepository(DataSource dataSource) {
        return instantiatePostgresRepository(
                "org.jobgovernance.storage.postgres.PostgresManualTriggerRequestRepository",
                dataSource,
                ManualTriggerRequestRepository.class
        );
    }

    @Bean
    @ConditionalOnBean(DataSource.class)
    @ConditionalOnClass(name = "org.jobgovernance.storage.postgres.PostgresAuditEventRepository")
    @ConditionalOnMissingBean
    public AuditEventRepository jgkAuditEventRepository(DataSource dataSource) {
        return instantiatePostgresRepository(
                "org.jobgovernance.storage.postgres.PostgresAuditEventRepository",
                dataSource,
                AuditEventRepository.class
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public JobRegistry jgkJobRegistry(List<JobRegistry.JobRegistration<?, ?>> registrations) {
        InMemoryJobRegistry registry = new InMemoryJobRegistry();
        registrations.forEach(registry::register);
        return registry;
    }

    @Bean
    @ConditionalOnMissingBean
    public ScheduleEvaluator jgkScheduleEvaluator() {
        return new DefaultScheduleEvaluator();
    }

    @Bean
    @ConditionalOnBean({JobRegistry.class, ScheduleEvaluator.class, ExecutionRepository.class})
    @ConditionalOnMissingBean
    public SchedulerMaterializationLoop jgkSchedulerMaterializationLoop(
            JobRegistry jobRegistry,
            ScheduleEvaluator scheduleEvaluator,
            ExecutionRepository executionRepository,
            ObjectProvider<ScheduleCursorRepository> scheduleCursorRepositoryProvider,
            ObjectProvider<JobDefinitionRepository> jobDefinitionRepositoryProvider,
            JgkProperties properties
    ) {
        return new SchedulerMaterializationLoop(
                jobRegistry,
                scheduleEvaluator,
                executionRepository,
                scheduleCursorRepositoryProvider.getIfAvailable(),
                jobDefinitionRepositoryProvider.getIfAvailable(),
                properties.scheduleBatchSize(),
                properties.schedulerInterval()
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public ActiveExecutionTracker jgkActiveExecutionTracker() {
        return new InMemoryActiveExecutionTracker();
    }

    @Bean(name = "jgkClaimedExecutionQueue")
    @ConditionalOnMissingBean(name = "jgkClaimedExecutionQueue")
    public BlockingQueue<JobExecution> jgkClaimedExecutionQueue(JgkProperties properties) {
        return new LinkedBlockingQueue<>(properties.maxClaimedNotStarted());
    }

    @Bean
    @ConditionalOnBean(ExecutionRepository.class)
    @ConditionalOnMissingBean
    public ClaimService jgkClaimService(ExecutionRepository executionRepository, JgkProperties properties) {
        return new RepositoryClaimService(executionRepository, properties.leaseTtl());
    }

    @Bean
    @ConditionalOnBean(ClaimService.class)
    @ConditionalOnMissingBean
    public PollingClaimLoop jgkPollingClaimLoop(
            ClaimService claimService,
            @Qualifier("jgkClaimedExecutionQueue") BlockingQueue<JobExecution> claimedQueue,
            JgkProperties properties
    ) {
        return new PollingClaimLoop(
                claimService,
                properties.workerId(),
                properties.claimBatchSize(),
                properties.claimPollBackoffMin(),
                properties.claimPollBackoffMax(),
                execution -> enqueueClaimedExecution(claimedQueue, execution)
        );
    }

    @Bean
    @ConditionalOnBean({JobRegistry.class, ExecutionRepository.class})
    @ConditionalOnMissingBean
    public HandlerRunnerLoop jgkHandlerRunnerLoop(
            @Qualifier("jgkClaimedExecutionQueue") BlockingQueue<JobExecution> claimedQueue,
            JobRegistry jobRegistry,
            ExecutionRepository executionRepository,
            ActiveExecutionTracker activeExecutionTracker,
            JgkProperties properties
    ) {
        return new HandlerRunnerLoop(
                claimedQueue,
                jobRegistry,
                executionRepository,
                properties.workerId(),
                properties.maxRunningGlobal(),
                activeExecutionTracker
        );
    }

    @Bean
    @ConditionalOnBean(ExecutionRepository.class)
    @ConditionalOnMissingBean
    public HeartbeatLoop jgkHeartbeatLoop(
            ExecutionRepository executionRepository,
            ActiveExecutionTracker activeExecutionTracker,
            JgkProperties properties
    ) {
        return new HeartbeatLoop(executionRepository, activeExecutionTracker, properties.heartbeatInterval());
    }

    @Bean
    @ConditionalOnBean(ExecutionRepository.class)
    @ConditionalOnMissingBean
    public RetryRequeueLoop jgkRetryRequeueLoop(ExecutionRepository executionRepository, JgkProperties properties) {
        return new RetryRequeueLoop(
                executionRepository,
                properties.retryRequeueInterval(),
                properties.retryRequeueBatchSize()
        );
    }

    @Bean
    @ConditionalOnBean(ExecutionRepository.class)
    @ConditionalOnMissingBean
    public RecoveryLoop jgkRecoveryLoop(ExecutionRepository executionRepository, JgkProperties properties) {
        return new RecoveryLoop(
                executionRepository,
                properties.workerId() + "-recovery",
                properties.recoveryInterval(),
                properties.recoveryDeadReason()
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public ExecutionEngine jgkExecutionEngine(List<ExecutionEngine.RunnableLoop> loops) {
        return new ExecutionEngine(loops);
    }

    @Bean
    @ConditionalOnBean({JobRegistry.class, ExecutionRepository.class})
    @ConditionalOnMissingBean
    public JobGovernanceManagementService jgkManagementService(
            JobRegistry jobRegistry,
            ExecutionRepository executionRepository,
            ObjectProvider<JobDefinitionRepository> jobDefinitionRepositoryProvider,
            ObjectProvider<ManualTriggerRequestRepository> triggerRequestRepositoryProvider,
            ObjectProvider<AuditEventRepository> auditEventRepositoryProvider
    ) {
        return new DefaultJobGovernanceManagementService(
                jobRegistry,
                executionRepository,
                jobDefinitionRepositoryProvider.getIfAvailable(),
                triggerRequestRepositoryProvider.getIfAvailable(),
                auditEventRepositoryProvider.getIfAvailable(),
                java.time.Clock.systemUTC()
        );
    }

    @Bean
    @ConditionalOnProperty(prefix = "jgk", name = "auto-start", havingValue = "true", matchIfMissing = true)
    @ConditionalOnMissingBean(name = "jgkExecutionEngineLifecycle")
    public ExecutionEngineLifecycle jgkExecutionEngineLifecycle(ExecutionEngine executionEngine) {
        return new ExecutionEngineLifecycle(executionEngine);
    }

    @Bean
    @ConditionalOnBean({ExecutionEngine.class, ActiveExecutionTracker.class, JobRegistry.class})
    @ConditionalOnMissingBean
    public JobGovernanceRuntimeStatusService jgkRuntimeStatusService(
            ExecutionEngine executionEngine,
            ActiveExecutionTracker activeExecutionTracker,
            JobRegistry jobRegistry,
            JgkProperties properties
    ) {
        return new DefaultJobGovernanceRuntimeStatusService(
                executionEngine,
                activeExecutionTracker,
                jobRegistry,
                properties.workerId(),
                java.time.Clock.systemUTC()
        );
    }

    private static void enqueueClaimedExecution(BlockingQueue<JobExecution> queue, JobExecution execution) {
        try {
            queue.put(execution);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while enqueueing claimed execution", interruptedException);
        }
    }

    private static <T> T instantiatePostgresRepository(String className, DataSource dataSource, Class<T> expectedType) {
        try {
            Class<?> repositoryClass = Class.forName(className);
            Object instance = repositoryClass.getConstructor(DataSource.class).newInstance(dataSource);
            return expectedType.cast(instance);
        } catch (ReflectiveOperationException exception) {
            throw new IllegalStateException("Failed to instantiate JGK PostgreSQL repository " + className, exception);
        }
    }
}
