package com.latticeengines.domain.exposed.modelquality;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PipelineStepOrFile {

    @JsonProperty("pipeline_step")
    public String pipelineStepName;
    
    @JsonProperty("pipeline_step_dir")
    public String pipelineStepDir;
}
