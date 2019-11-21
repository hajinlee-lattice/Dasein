package com.latticeengines.domain.exposed.datacloud.orchestration;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

/**
 * Keep doc
 * https://confluence.lattice-engines.com/display/ENG/DataCloud+Engine+Architecture#DataCloudEngineArchitecture-Orchestration
 * up to date if there is any new change
 *
 * Trigger strategy: Trigger when there is a new version of finished predefined
 * engine job
 *
 * Version strategy: Use latest version of finished predefined engine job
 * (ingested source version / transformation pipeline version / published source
 * version)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ExternalTriggerConfig extends OrchestrationConfig {
    // Type of predefined engine job on which orchestrated job pipeline depends
    @JsonProperty("Engine")
    private DataCloudEngine engine;

    // Name of predefined engine job on which orchestrated job pipeline depends,
    // eg. existed ingestion name / transformation pipeline name / publication
    // name
    @JsonProperty("EngineName")
    private String engineName;

    // LASTEST_VERSION: If there are multiple versions of finished predefined
    // engine job, only trigger engine pipeline ONCE with version same as latest
    // version of predefined engine job
    @JsonProperty("TriggerStrategy")
    private TriggerStrategy strategy;

    public DataCloudEngine getEngine() {
        return engine;
    }

    public void setEngine(DataCloudEngine engine) {
        this.engine = engine;
    }

    public String getEngineName() {
        return engineName;
    }

    public void setEngineName(String engineName) {
        this.engineName = engineName;
    }

    public TriggerStrategy getStrategy() {
        return strategy;
    }

    public void setStrategy(TriggerStrategy strategy) {
        this.strategy = strategy;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public enum TriggerStrategy {
        LATEST_VERSION
    }
}
