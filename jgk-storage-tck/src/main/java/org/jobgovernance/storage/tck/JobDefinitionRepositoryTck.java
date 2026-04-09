package org.jobgovernance.storage.tck;

import org.jobgovernance.core.model.JobDefinitionState;
import org.jobgovernance.storage.spi.JobDefinitionRepository;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public interface JobDefinitionRepositoryTck extends StorageContractSupport {

    JobDefinitionRepository jobDefinitionRepository();

    @Test
    default void shouldUpsertAndFindDefinition() {
        clearStorage();
        Instant now = fixedNow();
        var definition = sampleDefinition("tck.definition.upsert", JobDefinitionState.ENABLED);

        jobDefinitionRepository().upsert(definition, now);

        var loaded = jobDefinitionRepository().findByJobKey(definition.jobKey());
        assertTrue(loaded.isPresent());
        assertEquals(definition.jobKey(), loaded.get().jobKey());
        assertEquals(JobDefinitionState.ENABLED, loaded.get().state());
        assertEquals(definition.executionMode(), loaded.get().executionMode());
    }

    @Test
    default void shouldListOnlyEnabledDefinitions() {
        clearStorage();
        Instant now = fixedNow();
        jobDefinitionRepository().upsert(sampleDefinition("tck.definition.enabled", JobDefinitionState.ENABLED), now);
        jobDefinitionRepository().upsert(sampleDefinition("tck.definition.paused", JobDefinitionState.PAUSED), now);

        List<String> keys = jobDefinitionRepository().findEnabled(10).stream().map(definition -> definition.jobKey()).toList();
        assertEquals(List.of("tck.definition.enabled"), keys);
    }

    @Test
    default void shouldUpdateStateAndVersion() {
        clearStorage();
        Instant now = fixedNow();
        String jobKey = "tck.definition.state";
        jobDefinitionRepository().upsert(sampleDefinition(jobKey, JobDefinitionState.ENABLED), now);

        boolean updated = jobDefinitionRepository().updateState(jobKey, JobDefinitionState.PAUSED, "tck", now.plusSeconds(1));
        boolean missing = jobDefinitionRepository().updateState("missing.job", JobDefinitionState.PAUSED, "tck", now.plusSeconds(1));

        assertTrue(updated);
        assertFalse(missing);

        var loaded = jobDefinitionRepository().findByJobKey(jobKey).orElseThrow();
        assertEquals(JobDefinitionState.PAUSED, loaded.state());
        assertEquals(2, loaded.version());
    }
}
