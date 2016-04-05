package com.latticeengines.serviceflows.workflow.dataflow;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;

public class DataFlowStepConfiguration extends MicroserviceStepConfiguration {

    private Map<String, String> extraSources = new HashMap<>();

    @NotEmptyString
    @NotNull
    private String beanName;

    @NotEmptyString
    @NotNull
    private String targetTableName;

    private DataFlowParameters dataFlowParams;

    private boolean purgeSources;

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

    @JsonProperty("target_table_name")
    public String getTargetTableName() {
        return targetTableName;
    }

    @JsonProperty("target_table_name")
    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
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
    public boolean getPurgeSources() {
        return purgeSources;
    }

    @JsonProperty("purge_sources")
    public void setPurgeSources(boolean purgeSources) {
        this.purgeSources = purgeSources;
    }
}
