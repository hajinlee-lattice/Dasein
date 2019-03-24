package com.latticeengines.domain.exposed.datacloud.orchestration;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ExternalTriggerConfig extends OrchestrationConfig {
    @JsonProperty("Engine")
    private DataCloudEngine engine;

    @JsonProperty("EngineName")
    private String engineName;

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
