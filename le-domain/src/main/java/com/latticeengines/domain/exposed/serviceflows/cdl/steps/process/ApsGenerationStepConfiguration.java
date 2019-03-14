package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.serviceflows.core.steps.SparkScriptStepConfiguration;

public class ApsGenerationStepConfiguration extends SparkScriptStepConfiguration {

    @JsonProperty("rolling_period")
    private String rollingPeriod;

    @JsonProperty("force_rebuild")
    private Boolean forceRebuild;

    public String getRollingPeriod() {
        return rollingPeriod;
    }

    public void setRollingPeriod(String rollingPeriod) {
        this.rollingPeriod = rollingPeriod;
    }

    public Boolean getForceRebuild() {
        return forceRebuild;
    }

    public void setForceRebuild(Boolean forceRebuild) {
        this.forceRebuild = forceRebuild;
    }
}
