package com.latticeengines.serviceflows.workflow.dataflow;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;

public class DataFlowStepConfiguration extends MicroserviceStepConfiguration {

    @NotEmptyString
    @NotNull
    private String targetPath;

    @NotNull
    private Map<String, String> extraSources = null;

    @JsonProperty("target_path")
    public String getTargetPath() {
        return targetPath;
    }

    @JsonProperty("target_path")
    public void setTargetPath(String targetPath) {
        this.targetPath = targetPath;
    }

    @JsonProperty("extra_sources")
    public Map<String, String> getExtraSources() {
        return extraSources;
    }

    @JsonProperty("extra_sources")
    public void setExtraSources(Map<String, String> extraSources) {
        this.extraSources = extraSources;
    }
}
