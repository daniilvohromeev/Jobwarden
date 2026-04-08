package org.jobgovernance.spring.core;

import org.jobgovernance.executor.ExecutionEngine;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.List;

@AutoConfiguration
@EnableConfigurationProperties(JgkProperties.class)
@ConditionalOnProperty(prefix = "jgk", name = "enabled", havingValue = "true", matchIfMissing = true)
public class JobGovernanceAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public ExecutionEngine jgkExecutionEngine(List<ExecutionEngine.RunnableLoop> loops) {
        return new ExecutionEngine(loops);
    }
}
