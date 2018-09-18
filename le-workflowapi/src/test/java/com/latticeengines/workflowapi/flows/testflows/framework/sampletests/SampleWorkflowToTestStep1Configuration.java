package com.latticeengines.workflowapi.flows.testflows.framework.sampletests;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class SampleWorkflowToTestStep1Configuration extends BaseStepConfiguration {

    @JsonProperty("customer_space")
    private String customerSpace;

    public String getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(String customerSpace) {
        this.customerSpace = customerSpace;
    }
}
