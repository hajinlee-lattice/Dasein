package com.latticeengines.serviceflows.workflow.dataflow;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;

public class DataFlowStepConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("extra_sources")
    private Map<String, String> extraSources = new HashMap<>();

    @NotEmptyString
    @NotNull
    @JsonProperty("bean_name")
    private String beanName;

    @NotEmptyString
    @NotNull
    @JsonProperty("target_table_name")
    private String targetTableName;

    @JsonProperty("target_path")
    private String targetPath;

    @JsonProperty("partitions")
    private Integer partitions;

    @JsonProperty("data_flow_params")
    private DataFlowParameters dataFlowParams;

    @JsonProperty("job_properties")
    private Properties jobProperties;

    @JsonProperty("engine")
    private String engine;
    
    @JsonProperty("queue")
    private String queue;
    
    @JsonProperty("namespace")
    private String namespace;

    @JsonProperty("user_id")
    private String userId;

    
    public Map<String, String> getExtraSources() {
        return extraSources;
    }

    public void setExtraSources(Map<String, String> extraSources) {
        this.extraSources = extraSources;
    }

    
    public String getBeanName() {
        return beanName;
    }

    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }


    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    
    public String getTargetPath() {
        return targetPath;
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

    public DataFlowParameters getDataFlowParams() {
        return dataFlowParams;
    }

    public void setDataFlowParams(DataFlowParameters dataFlowParams) {
        this.dataFlowParams = dataFlowParams;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

}
