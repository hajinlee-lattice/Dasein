package com.latticeengines.domain.exposed.serviceflows.datacloud;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class BaseDataCloudWorkflowConfiguration extends WorkflowConfiguration {

    @Override
    public String getSwpkgName() {
        return "datacloud";
    }

}
