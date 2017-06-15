package com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.manage.Orchestration;
import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;
import com.latticeengines.domain.exposed.datacloud.orchestration.OrchestrationConfig;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class OrchestrationStepConfig extends BaseStepConfiguration {
    @NotNull
    private Orchestration orchestration;

    @NotNull
    private OrchestrationProgress orchestrationProgress;

    @NotNull
    private OrchestrationConfig orchestrationConfig;

    @JsonProperty("orchestration")
    public Orchestration getOrchestration() {
        return orchestration;
    }

    @JsonProperty("orchestration")
    public void setOrchestration(Orchestration orchestration) {
        this.orchestration = orchestration;
    }

    @JsonProperty("orchestration_progress")
    public OrchestrationProgress getOrchestrationProgress() {
        return orchestrationProgress;
    }

    @JsonProperty("orchestration_progress")
    public void setOrchestrationProgress(OrchestrationProgress orchestrationProgress) {
        this.orchestrationProgress = orchestrationProgress;
    }

    @JsonProperty("orchestration_config")
    public OrchestrationConfig getOrchestrationConfig() {
        return orchestrationConfig;
    }

    @JsonProperty("orchestration_config")
    public void setOrchestrationConfig(OrchestrationConfig orchestrationConfig) {
        this.orchestrationConfig = orchestrationConfig;
    }

}
