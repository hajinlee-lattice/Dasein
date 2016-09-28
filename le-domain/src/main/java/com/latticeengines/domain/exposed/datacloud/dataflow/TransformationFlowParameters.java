package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class TransformationFlowParameters extends DataFlowParameters {

    @JsonProperty("ConfJsonPath")
    private String confJsonPath;

    @JsonProperty("FakedCurrentTime")
    private Date fakedCurrentTime;

    public String getConfJsonPath() {
        return confJsonPath;
    }

    public void setConfJsonPath(String confJsonPath) {
        this.confJsonPath = confJsonPath;
    }

    public Date getFakedCurrentTime() {
        return fakedCurrentTime;
    }

    public void setFakedCurrentTime(Date fakedCurrentTime) {
        this.fakedCurrentTime = fakedCurrentTime;
    }
}
