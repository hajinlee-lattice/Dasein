package com.latticeengines.domain.exposed.datacloud.transformation;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PipelineTransformationReport {

    @JsonProperty("Submitter")
    private String submitter;

    @JsonProperty("Name")
    private String name;

    @JsonProperty("Steps")
    private List<TransformationStepReport> steps;

    public String getSubmitter() {
        return submitter;
    }

    public void setSubmitter(String creator) {
        this.submitter = creator;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<TransformationStepReport> getSteps() {
        return steps;
    }

    public void setSteps(List<TransformationStepReport> steps) {
        this.steps = steps;
    }
}
