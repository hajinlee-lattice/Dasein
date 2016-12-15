package com.latticeengines.domain.exposed.datacloud.transformation;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PipelineTransformationRequest {

    @JsonProperty("Submitter")
    private String submitter;

    @JsonProperty("Steps")
    private List<TransformationStepConfig> steps;

    public String getSubmitter() {
        return submitter;
    }

    public void setSubmitter(String creator) {
        this.submitter = creator;
    }

    public List<TransformationStepConfig> getSteps() {
        return steps;
    }

    public void setSteps(List<TransformationStepConfig> steps) {
        this.steps = steps;
    }
}
