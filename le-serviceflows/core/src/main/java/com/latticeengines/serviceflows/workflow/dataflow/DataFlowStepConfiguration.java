package com.latticeengines.serviceflows.workflow.dataflow;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;

public class DataFlowStepConfiguration extends MicroserviceStepConfiguration {

    @NotEmptyString
    @NotNull
    private String targetPath;

    private Map<String, String> extraSources = new HashMap<>();

    @NotEmptyString
    @NotNull
    private String beanName;

    @NotEmptyString
    @NotNull
    private String name;

    private DataFlowParameters dataFlowParams;

    private boolean purgeSources;

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

    @JsonProperty("bean_name")
    public String getBeanName() {
        return beanName;
    }

    @JsonProperty("bean_name")
    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("data_flow_params")
    public DataFlowParameters getDataFlowParams() {
        return dataFlowParams;
    }

    @JsonProperty("data_flow_params")
    public void setDataFlowParams(DataFlowParameters dataFlowParams) {
        this.dataFlowParams = dataFlowParams;
    }

    @JsonProperty("purge_sources")
    public Boolean getPurgeSources() {
        return purgeSources;
    }

    @JsonProperty("purge_sources")
    public void setPurgeSources(Boolean purgeSources) {
        this.purgeSources = purgeSources;
    }
}
