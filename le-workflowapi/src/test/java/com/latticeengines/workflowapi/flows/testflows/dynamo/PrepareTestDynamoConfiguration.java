package com.latticeengines.workflowapi.flows.testflows.dynamo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class PrepareTestDynamoConfiguration extends BaseStepConfiguration {

    @JsonProperty("customer_space")
    private String customerSpace;

    @JsonProperty("update_mode")
    private boolean updateMode;

    public String getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(String customerSpace) {
        this.customerSpace = customerSpace;
    }

    public boolean isUpdateMode() {
        return updateMode;
    }

    public void setUpdateMode(boolean updateMode) {
        this.updateMode = updateMode;
    }

}
