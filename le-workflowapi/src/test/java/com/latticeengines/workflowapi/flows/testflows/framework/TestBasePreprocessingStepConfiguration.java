package com.latticeengines.workflowapi.flows.testflows.framework;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class TestBasePreprocessingStepConfiguration extends BaseStepConfiguration {

    public TestBasePreprocessingStepConfiguration() {}

    public TestBasePreprocessingStepConfiguration(String stepBeanName) {
        this.stepBeanName = stepBeanName;
    }

    @JsonProperty("customer_space")
    private String customerSpace;

    @JsonProperty("step_bean_name")
    private String stepBeanName;

    public String getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(String customerSpace) {
        this.customerSpace = customerSpace;
    }

    public String getStepBeanName() { return stepBeanName; }

    public void setStepBeanName(String stepBeanName) { this.stepBeanName = stepBeanName; }
}
