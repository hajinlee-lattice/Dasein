package com.latticeengines.domain.exposed.workflow;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.BasePayloadConfiguration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

public class WorkflowConfiguration extends BasePayloadConfiguration {

    private Map<String, String> configRegistry = new HashMap<>();

    @JsonProperty("workflowName")
    private String workflowName;

    @JsonProperty("internalResourceHostPort")
    private String internalResourceHostPort;

    @JsonProperty("containerMemMB")
    private Integer containerMemoryMB;

    @JsonProperty
    private boolean restart;

    @JsonProperty
    private WorkflowExecutionId workflowIdToRestart;

    @JsonProperty
    private Map<String, String> inputProperties = new HashMap<>();

    protected void add(BaseStepConfiguration configuration) {
        configRegistry.put(configuration.getClass().getName(), configuration.toString());
    }

    protected void add(WorkflowConfiguration configuration) {
        configRegistry.putAll(configuration.configRegistry);
    }

    public Map<String, String> getConfigRegistry() {
        return configRegistry;
    }

    public void setContainerConfiguration(String workflowName, CustomerSpace customerSpace, String payloadName) {
        this.workflowName = workflowName;
        setCustomerSpace(customerSpace);
        setName(payloadName);
    }

    public String getWorkflowName() {
        return workflowName;
    }

    public void setWorkflowName(String workflowName) {
        this.workflowName = workflowName;
    }

    public boolean isRestart() {
        return restart;
    }

    public void setRestart(boolean restart) {
        this.restart = restart;
    }

    public WorkflowExecutionId getWorkflowIdToRestart() {
        return workflowIdToRestart;
    }

    public void setWorkflowIdToRestart(WorkflowExecutionId workflowIdToRestart) {
        this.workflowIdToRestart = workflowIdToRestart;
    }

    public String getInputPropertyValue(String key) {
        return inputProperties.get(key);
    }

    public void setInputPropertyValue(String key, String value) {
        inputProperties.put(key, value);
    }

    public Map<String, String> getInputProperties() {
        return inputProperties;
    }

    public void setInputProperties(Map<String, String> inputProperties) {
        this.inputProperties = inputProperties;
    }

    public String getInternalResourceHostPort() {
        return internalResourceHostPort;
    }

    public void setInternalResourceHostPort(String internalResourceHostPort) {
        this.internalResourceHostPort = internalResourceHostPort;
    }

    public Integer getContainerMemoryMB() {
        return containerMemoryMB;
    }

    public void setContainerMemoryMB(Integer containerMemoryMB) {
        this.containerMemoryMB = containerMemoryMB;
    }

    @JsonIgnore
    public Map<String, Class<?>> getStepConfigClasses() {
        return Collections.emptyMap();
    }

}
