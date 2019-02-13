package com.latticeengines.domain.exposed.workflow;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.BasePayloadConfiguration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.BaseDataCloudWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.BaseLPWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.modeling.BaseModelingWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.BaseScoringWorkflowConfiguration;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "name")
@JsonSubTypes({
        @Type(value = BaseCDLWorkflowConfiguration.class, name = "BaseCDLWorkflowConfiguration"),
        @Type(value = BaseDataCloudWorkflowConfiguration.class, name = "BaseDataCloudWorkflowConfiguration"),
        @Type(value = BaseLPWorkflowConfiguration.class, name = "BaseLPWorkflowConfiguration"),
        @Type(value = BaseModelingWorkflowConfiguration.class, name = "BaseModelingWorkflowConfiguration"),
        @Type(value = BaseScoringWorkflowConfiguration.class, name = "BaseScoringWorkflowConfiguration") })
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class WorkflowConfiguration extends BasePayloadConfiguration {

    private static final Logger log = LoggerFactory.getLogger(WorkflowConfiguration.class);

    @JsonProperty("stepConfigRegistry")
    private Map<String, String> stepConfigRegistry = new HashMap<>();

    @JsonProperty("subWorkflowConfigRegistry")
    private Map<String, WorkflowConfiguration> subWorkflowConfigRegistry = new HashMap<>();

    @JsonProperty("workflowName")
    private String workflowName;

    @JsonProperty("internalResourceHostPort")
    private String internalResourceHostPort;

    @JsonProperty("containerMemMB")
    private Integer containerMemoryMB;

    private Collection<String> swpkgNames;

    @JsonProperty
    private boolean restart;

    @JsonProperty
    private boolean skipCompletedSteps = false;

    @JsonProperty
    private WorkflowExecutionId workflowIdToRestart;

    @JsonProperty
    private Map<String, String> inputProperties = new HashMap<>();

    @JsonProperty("falingStep")
    private FailingStep failingStep;

    @JsonProperty("swpkgNames")
    public Collection<String> getSwpkgNames() {
        return swpkgNames;
    }

    @JsonProperty("swpkgNames")
    public void setSwpkgNames(Collection<String> swpkgNames) {
        this.swpkgNames = swpkgNames;
    }

    public void add(BaseStepConfiguration configuration) {
        log.info("Added " + configuration.getClass().getSimpleName() + " to " + getClass().getSimpleName());
        stepConfigRegistry.put(configuration.getClass().getSimpleName(), configuration.toString());
    }

    protected void add(WorkflowConfiguration configuration) {
        log.info("Added " + configuration.getWorkflowName() + " to " + getClass().getSimpleName());
        subWorkflowConfigRegistry.put(configuration.getWorkflowName(), configuration);
    }

    public Map<String, String> getStepConfigRegistry() {
        return stepConfigRegistry;
    }

    public Map<String, WorkflowConfiguration> getSubWorkflowConfigRegistry() {
        return subWorkflowConfigRegistry;
    }

    public void setContainerConfiguration(String workflowName, CustomerSpace customerSpace,
            String payloadName) {
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

    public boolean isSkipCompletedSteps() {
        return skipCompletedSteps;
    }

    public void setSkipCompletedSteps(boolean skipCompletedSteps) {
        this.skipCompletedSteps = skipCompletedSteps;
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

    public FailingStep getFailingStep() {
        return failingStep;
    }

    public void setFailingStep(FailingStep failingStep) {
        this.failingStep = failingStep;
    }

    @JsonIgnore
    public WorkflowConfiguration getSubWorkflowConfiguration(String workflowName) {
        return this.getSubWorkflowConfigRegistry().get(workflowName);
    }

}
