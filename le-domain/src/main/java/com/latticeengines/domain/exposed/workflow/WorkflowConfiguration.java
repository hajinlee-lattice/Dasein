package com.latticeengines.domain.exposed.workflow;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.BasePayloadConfiguration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

public class WorkflowConfiguration extends BasePayloadConfiguration {

    private Map<String, String> configRegistry = new HashMap<>();

    @JsonProperty("workflowName")
    private String workflowName;

    protected void add(BaseStepConfiguration configuration) {
        configRegistry.put(configuration.getClass().getName(), configuration.toString());
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

}
