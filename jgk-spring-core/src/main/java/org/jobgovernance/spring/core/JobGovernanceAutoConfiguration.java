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
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@AutoConfiguration
@EnableConfigurationProperties(JgkProperties.class)
@ConditionalOnProperty(prefix = "jgk", name = "enabled", havingValue = "true", matchIfMissing = true)
public class JobGovernanceAutoConfiguration {

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
            JgkProperties properties
    ) {
        return new SchedulerMaterializationLoop(
                jobRegistry,
                scheduleEvaluator,
                executionRepository,
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
    @ConditionalOnProperty(prefix = "jgk", name = "auto-start", havingValue = "true", matchIfMissing = true)
    @ConditionalOnMissingBean(name = "jgkExecutionEngineLifecycle")
    public ExecutionEngineLifecycle jgkExecutionEngineLifecycle(ExecutionEngine executionEngine) {
        return new ExecutionEngineLifecycle(executionEngine);
    }

    private static void enqueueClaimedExecution(BlockingQueue<JobExecution> queue, JobExecution execution) {
        try {
            queue.put(execution);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while enqueueing claimed execution", interruptedException);
        }
    }
}
