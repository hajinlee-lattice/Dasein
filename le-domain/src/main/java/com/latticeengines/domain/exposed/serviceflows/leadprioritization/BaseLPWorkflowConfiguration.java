package com.latticeengines.domain.exposed.serviceflows.leadprioritization;

import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class BaseLPWorkflowConfiguration extends WorkflowConfiguration {

    @Override
    public String getSwpkgName() {
        return SoftwareLibrary.LeadPrioritization.getName();
    }

}
