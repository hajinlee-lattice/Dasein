package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ScoringRequestConfigSummary {

    protected String configId;
    protected String modelUuid;
    
    public ScoringRequestConfigSummary() {
        
    }
    
    public ScoringRequestConfigSummary(String configId, String modelUuid) {
        this.configId = configId;
        this.modelUuid = modelUuid;
    }
    
    @JsonProperty("requestConfigId")
    public String getConfigId() {
        return configId;
    }

    public void setConfigId(String configId) {
        this.configId = configId;
    }

    @JsonProperty("modelUuid")
    public String getModelUuid() {
        return modelUuid;
    }

    public void setModelUuid(String modelUuid) {
        this.modelUuid = modelUuid;
    }
}
