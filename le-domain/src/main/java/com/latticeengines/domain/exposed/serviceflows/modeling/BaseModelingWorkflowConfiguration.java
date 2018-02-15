package com.latticeengines.domain.exposed.serviceflows.modeling;

import java.util.Collection;
import java.util.Collections;

import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class BaseModelingWorkflowConfiguration extends WorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return Collections.singleton(SoftwareLibrary.Modeling.getName());
    }

}
