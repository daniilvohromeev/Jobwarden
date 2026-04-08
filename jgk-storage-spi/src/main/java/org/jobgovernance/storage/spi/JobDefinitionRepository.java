package org.jobgovernance.storage.spi;

import org.jobgovernance.core.model.JobDefinition;
import org.jobgovernance.core.model.JobDefinitionState;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface JobDefinitionRepository {

    void upsert(JobDefinition definition, Instant now);

    Optional<JobDefinition> findByJobKey(String jobKey);

    List<JobDefinition> findEnabled(int limit);

    boolean updateState(String jobKey, JobDefinitionState targetState, String actor, Instant changedAt);
}
