package com.latticeengines.cdl.workflow.steps;

import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;

public class StartExecutionConfiguration extends MicroserviceStepConfiguration {

    private String datafeedName;

    public String getDataFeedName() {
        return datafeedName;
    }

    public void setDataFeedName(String datafeedName) {
        this.datafeedName = datafeedName;
    }

}
