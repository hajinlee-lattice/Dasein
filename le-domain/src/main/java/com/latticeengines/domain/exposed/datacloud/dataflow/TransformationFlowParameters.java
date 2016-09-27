package com.latticeengines.domain.exposed.datacloud.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class TransformationFlowParameters extends DataFlowParameters {

    @JsonProperty("ConfJsonPath")
    private String confJsonPath;

    public String getConfJsonPath() {
        return confJsonPath;
    }

    public void setConfJsonPath(String confJsonPath) {
        this.confJsonPath = confJsonPath;
    }
}
