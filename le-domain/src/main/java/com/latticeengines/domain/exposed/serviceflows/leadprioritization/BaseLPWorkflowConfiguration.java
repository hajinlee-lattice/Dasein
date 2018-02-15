package com.latticeengines.domain.exposed.serviceflows.leadprioritization;

import java.util.Collection;
import java.util.Collections;

import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class BaseLPWorkflowConfiguration extends WorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return Collections.singleton(SoftwareLibrary.LeadPrioritization.getName());
    }

}
