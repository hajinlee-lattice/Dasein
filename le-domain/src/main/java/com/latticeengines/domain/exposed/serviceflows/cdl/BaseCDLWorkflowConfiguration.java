package com.latticeengines.domain.exposed.serviceflows.cdl;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class BaseCDLWorkflowConfiguration extends WorkflowConfiguration {

    @Override
    public String getSwpkgName() {
        return "cdl";
    }

}
