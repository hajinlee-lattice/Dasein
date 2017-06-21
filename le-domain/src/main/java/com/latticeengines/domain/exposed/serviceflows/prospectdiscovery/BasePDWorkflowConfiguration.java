package com.latticeengines.domain.exposed.serviceflows.prospectdiscovery;

import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class BasePDWorkflowConfiguration extends WorkflowConfiguration {

    @Override
    public String getSwpkgName() {
        return SoftwareLibrary.ProspectDiscovery.getName();
    }

}
