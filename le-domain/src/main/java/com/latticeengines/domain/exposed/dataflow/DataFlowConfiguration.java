package com.latticeengines.domain.exposed.dataflow;

import java.util.List;
import java.util.Properties;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.BasePayloadConfiguration;

public class DataFlowConfiguration extends BasePayloadConfiguration {

    @JsonProperty("bean_name")
    private String dataFlowBeanName;

    @JsonProperty("sources")
    private List<DataFlowSource> dataSources;

    @JsonProperty("data_flow_parameters")
    private DataFlowParameters dataFlowParameters;

    @JsonProperty("target_table_name")
    private String targetTableName;

    @JsonProperty("target_path")
    private String targetPath;

    @JsonProperty("partitions")
    private Integer partitions;

    @JsonProperty("job_properties")
    private Properties jobProperties;

    @JsonProperty("engine")
    private String engine;

    @JsonProperty("queue")
    private String queue;

    @JsonProperty("namespace")
    private String namespace;

    @JsonProperty("swlib")
    private String swlib;

    @JsonProperty("skip_registering_table")
    private boolean skipRegisteringTable = false;

    @JsonProperty("apply_table_properties")
    private boolean applyTableProperties = false;

    @JsonProperty("am_mem_gb")
    private Integer amMemGb;

    @JsonProperty("am_vcores")
    private Integer amVcores;

    public String getDataFlowBeanName() {
        return dataFlowBeanName;
    }

    public void setDataFlowBeanName(String dataFlowBeanName) {
        this.dataFlowBeanName = dataFlowBeanName;
    }

    public List<DataFlowSource> getDataSources() {
        return dataSources;
    }

    public void setDataSources(List<DataFlowSource> dataSources) {
        this.dataSources = dataSources;
    }

    public DataFlowParameters getDataFlowParameters() {
        return dataFlowParameters;
    }

    public void setDataFlowParameters(DataFlowParameters dataFlowParameters) {
        this.dataFlowParameters = dataFlowParameters;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    public String getTargetPath() {
        return this.targetPath;
    }

    public void setTargetPath(String targetPath) {
        this.targetPath = targetPath;
    }

    public Integer getPartitions() {
        return partitions;
    }

    public void setPartitions(Integer partitions) {
        this.partitions = partitions;
    }

    public Properties getJobProperties() {
        return jobProperties;
    }

    public void setJobProperties(Properties jobProperties) {
        this.jobProperties = jobProperties;
    }

    public String getEngine() {
        return this.engine;
    }

    public void setEngine(String engine) {
        this.engine = engine;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public boolean shouldSkipRegisteringTable() {
        return skipRegisteringTable;
    }

    public void setSkipRegisteringTable(boolean skipRegisteringTable) {
        this.skipRegisteringTable = skipRegisteringTable;
    }

    public String getSwlib() {
        return swlib;
    }

    public void setSwlib(String swlib) {
        this.swlib = swlib;
    }

    public boolean isApplyTableProperties() {
        return applyTableProperties;
    }

    public void setApplyTableProperties(boolean applyTableProperties) {
        this.applyTableProperties = applyTableProperties;
    }

    public Integer getAmMemGb() {
        return amMemGb;
    }

    public void setAmMemGb(Integer amMemGb) {
        this.amMemGb = amMemGb;
    }

    public Integer getAmVcores() {
        return amVcores;
    }

    public void setAmVcores(Integer amVcores) {
        this.amVcores = amVcores;
    }
}
