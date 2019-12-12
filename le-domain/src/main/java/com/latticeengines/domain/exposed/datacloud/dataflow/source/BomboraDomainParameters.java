package com.latticeengines.domain.exposed.datacloud.dataflow.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;

public class BomboraDomainParameters extends TransformationFlowParameters {

    @JsonProperty("CurrentRecords")
    private Long currentRecords;

    @JsonProperty("SourceName")
    private String sourceName;

    public Long getCurrentRecords() {
        return currentRecords;
    }

    public void setCurrentRecords(Long currentRecords) {
        this.currentRecords = currentRecords;
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

}
