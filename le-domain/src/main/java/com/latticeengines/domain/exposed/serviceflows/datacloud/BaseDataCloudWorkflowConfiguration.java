package com.latticeengines.domain.exposed.serviceflows.datacloud;

import java.util.Collection;
import java.util.Collections;

import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class BaseDataCloudWorkflowConfiguration extends WorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return Collections.singleton(SoftwareLibrary.DataCloud.getName());
    }

}
