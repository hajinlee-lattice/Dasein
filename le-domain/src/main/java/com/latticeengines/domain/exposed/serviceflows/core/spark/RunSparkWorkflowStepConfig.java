package com.latticeengines.domain.exposed.serviceflows.core.spark;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.domain.exposed.workflow.RunSparkWorkflowRequest;

public class RunSparkWorkflowStepConfig extends BaseStepConfiguration {

    @JsonProperty("customerSpace")
    private String customerSpace;

    @NotNull
    @JsonProperty("request")
    private RunSparkWorkflowRequest request;

    public String getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(String customerSpace) {
        this.customerSpace = customerSpace;
    }

    public RunSparkWorkflowRequest getRequest() {
        return request;
    }

    public void setRequest(RunSparkWorkflowRequest request) {
        this.request = request;
    }
}
