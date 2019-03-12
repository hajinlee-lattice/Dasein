package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.serviceflows.core.steps.SparkScriptStepConfiguration;

public class ApsGenerationStepConfiguration extends SparkScriptStepConfiguration {

    @JsonProperty("rolling_period")
    private String rollingPeriod;

    public String getRollingPeriod() {
        return rollingPeriod;
    }

    public void setRollingPeriod(String rollingPeriod) {
        this.rollingPeriod = rollingPeriod;
    }
}
