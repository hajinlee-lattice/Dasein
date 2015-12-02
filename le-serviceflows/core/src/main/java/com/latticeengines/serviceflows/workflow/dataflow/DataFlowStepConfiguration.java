package com.latticeengines.serviceflows.workflow.dataflow;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;

public class DataFlowStepConfiguration extends MicroserviceStepConfiguration {

    @NotEmptyString
    @NotNull
    private String targetPath;

    @NotNull
    private List<String> extraSources = null;

    @JsonProperty("target_path")
    public String getTargetPath() {
        return targetPath;
    }

    @JsonProperty("target_path")
    public void setTargetPath(String targetPath) {
        this.targetPath = targetPath;
    }

    @JsonProperty("extra_sources")
    public List<String> getExtraSources() {
        return extraSources;
    }

    @JsonProperty("extra_sources")
    public void setExtraSources(List<String> extraSources) {
        this.extraSources = extraSources;
    }
}
