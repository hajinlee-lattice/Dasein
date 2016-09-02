package com.latticeengines.domain.exposed.dataflow;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.BasePayloadConfiguration;

public class DataFlowConfiguration extends BasePayloadConfiguration {

    private String dataFlowBeanName;
    private List<DataFlowSource> dataSources;
    private DataFlowParameters dataFlowParameters;
    private String targetTableName;
    private String targetPath;
    private Integer partitions;

    @JsonProperty("bean_name")
    public String getDataFlowBeanName() {
        return dataFlowBeanName;
    }

    @JsonProperty("bean_name")
    public void setDataFlowBeanName(String dataFlowBeanName) {
        this.dataFlowBeanName = dataFlowBeanName;
    }

    @JsonProperty("sources")
    public List<DataFlowSource> getDataSources() {
        return dataSources;
    }

    @JsonProperty("sources")
    public void setDataSources(List<DataFlowSource> dataSources) {
        this.dataSources = dataSources;
    }

    @JsonProperty("data_flow_parameters")
    public DataFlowParameters getDataFlowParameters() {
        return dataFlowParameters;
    }

    @JsonProperty("data_flow_parameters")
    public void setDataFlowParameters(DataFlowParameters dataFlowParameters) {
        this.dataFlowParameters = dataFlowParameters;
    }

    @JsonProperty("target_table_name")
    public String getTargetTableName() {
        return targetTableName;
    }

    @JsonProperty("target_table_name")
    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    @JsonProperty("target_path")
    public String getTargetPath() {
        return this.targetPath;
    }

    @JsonProperty("target_path")
    public void setTargetPath(String targetPath) {
        this.targetPath = targetPath;
    }

    @JsonProperty("partitions")
    public Integer getPartitions() {
        return partitions;
    }

    @JsonProperty("partitions")
    public void setPartitions(Integer partitions) {
        this.partitions = partitions;
    }
    
}
