package com.latticeengines.domain.exposed.datacloud.orchestration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class ExternalTriggerConfig extends OrchestrationConfig {
    private DataCloudEngine engine;
    private String engineName;
    private TriggerStrategy strategy;

    @JsonProperty("Engine")
    public DataCloudEngine getEngine() {
        return engine;
    }

    @JsonProperty("Engine")
    public void setEngine(DataCloudEngine engine) {
        this.engine = engine;
    }

    @JsonProperty("EngineName")
    public String getEngineName() {
        return engineName;
    }

    @JsonProperty("EngineName")
    public void setEngineName(String engineName) {
        this.engineName = engineName;
    }

    @JsonProperty("TriggerStrategy")
    public TriggerStrategy getStrategy() {
        return strategy;
    }

    @JsonProperty("TriggerStrategy")
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
