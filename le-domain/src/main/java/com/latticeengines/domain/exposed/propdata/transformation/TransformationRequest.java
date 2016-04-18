package com.latticeengines.domain.exposed.propdata.transformation;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TransformationRequest {
    private String sourceBeanName;
    private String submitter;

    @JsonProperty("SourceBeanName")
    public String getSourceBeanName() {
        return sourceBeanName;
    }

    @JsonProperty("SourceBeanName")
    public void setSourceBeanName(String sourceBeanName) {
        this.sourceBeanName = sourceBeanName;
    }

    @JsonProperty("Submitter")
    public String getSubmitter() {
        return submitter;
    }

    @JsonProperty("Submitter")
    public void setSubmitter(String creator) {
        this.submitter = creator;
    }
}

