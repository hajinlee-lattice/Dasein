package com.latticeengines.workflowapi.flows.testflows.dynamo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.workflowapi.flows.testflows.framework.TestBasePreprocessingStepConfiguration;

public class PrepareTestDynamoConfiguration extends TestBasePreprocessingStepConfiguration {

    @JsonProperty("update_mode")
    private boolean updateMode;

    public PrepareTestDynamoConfiguration() {}

    public PrepareTestDynamoConfiguration(String stepBeanName) {
        super(stepBeanName);
    }

    public boolean isUpdateMode() {
        return updateMode;
    }

    public void setUpdateMode(boolean updateMode) {
        this.updateMode = updateMode;
    }
}
