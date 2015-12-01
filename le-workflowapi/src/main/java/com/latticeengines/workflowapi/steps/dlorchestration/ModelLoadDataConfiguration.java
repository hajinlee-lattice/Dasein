package com.latticeengines.workflowapi.steps.dlorchestration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class ModelLoadDataConfiguration extends BaseStepConfiguration {

    private int i;

    @JsonProperty("anint")
    public int getI() {
        return i;
    }

    @JsonProperty("anint")
    public void setI(int i) {
        this.i = i;
    }


}
