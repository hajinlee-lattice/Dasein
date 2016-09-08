package com.latticeengines.domain.exposed.modelquality;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PipelineJson {

    @JsonProperty("columnTransformFiles")
    private Map<String, PipelineStep> steps;

    public Map<String, PipelineStep> getSteps() {
        return steps;
    }

    public void setSteps(Map<String, PipelineStep> steps) {
        this.steps = steps;
    }
}
