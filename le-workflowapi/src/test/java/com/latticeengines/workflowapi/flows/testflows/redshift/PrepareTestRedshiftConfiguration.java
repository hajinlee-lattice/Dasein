package com.latticeengines.workflowapi.flows.testflows.redshift;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.workflowapi.flows.testflows.framework.TestBasePreprocessingStepConfiguration;

public class PrepareTestRedshiftConfiguration extends TestBasePreprocessingStepConfiguration {

    @JsonProperty("update_mode")
    private boolean updateMode;

    public PrepareTestRedshiftConfiguration() {}

    public PrepareTestRedshiftConfiguration(String stepBeanName) {
        super(stepBeanName);
    }

    public boolean isUpdateMode() {
        return updateMode;
    }

    public void setUpdateMode(boolean updateMode) {
        this.updateMode = updateMode;
    }
}
