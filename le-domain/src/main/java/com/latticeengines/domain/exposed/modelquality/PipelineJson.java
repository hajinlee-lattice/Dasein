package com.latticeengines.domain.exposed.modelquality;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PipelineJson {
    
    public PipelineJson() {}
    
    public PipelineJson(Map<String, PipelineStep> steps) {
        setSteps(steps);
    }

    @JsonProperty("columnTransformFiles")
    private Map<String, PipelineStep> steps = new HashMap<>();

    public Map<String, PipelineStep> getSteps() {
        return steps;
    }

    public void setSteps(Map<String, PipelineStep> steps) {
        this.steps = steps;
    }
}
