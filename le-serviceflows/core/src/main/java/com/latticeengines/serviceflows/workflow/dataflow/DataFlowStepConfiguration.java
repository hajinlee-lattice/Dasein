package com.latticeengines.serviceflows.workflow.dataflow;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;

public class DataFlowStepConfiguration extends MicroserviceStepConfiguration {

    @NotEmptyString
    @NotNull
    private String flowName;

    @NotEmptyString
    @NotNull
    private String dataflowBeanName;

    @NotEmptyString
    @NotNull
    private String targetPath;

    @NotNull
    private Map<String, String> extraSourceFileToPathMap = null;

    @JsonProperty("flow_name")
    public String getFlowName() {
        return flowName;
    }

    @JsonProperty("flow_name")
    public void setFlowName(String flowName) {
        this.flowName = flowName;
    }

    @JsonProperty("bean_name")
    public String getDataflowBeanName() {
        return dataflowBeanName;
    }

    @JsonProperty("bean_name")
    public void setDataflowBeanName(String dataflowBeanName) {
        this.dataflowBeanName = dataflowBeanName;
    }

    @JsonProperty("target_path")
    public String getTargetPath() {
        return targetPath;
    }

    @JsonProperty("target_path")
    public void setTargetPath(String targetPath) {
        this.targetPath = targetPath;
    }

    @JsonProperty("extra_sources")
    public Map<String, String> getExtraSourceFileToPathMap() {
        return extraSourceFileToPathMap;
    }

    @JsonProperty("extra_sources")
    public void setExtraSourceFileToPathMap(Map<String, String> extraSourceFileToPathMap) {
        this.extraSourceFileToPathMap = extraSourceFileToPathMap;
    }

}
