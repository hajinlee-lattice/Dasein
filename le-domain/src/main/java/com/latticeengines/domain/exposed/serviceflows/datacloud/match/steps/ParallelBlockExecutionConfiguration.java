package com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class ParallelBlockExecutionConfiguration extends MicroserviceStepConfiguration {
    
    
    private String resultLocation;

    @JsonProperty("resultLocation")
    public String getResultLocation() {
        return resultLocation;
    }

    @JsonProperty("resultLocation")
    public void setResultLocation(String resultLocation) {
        this.resultLocation = resultLocation;
    }
}
