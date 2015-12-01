package com.latticeengines.pls.entitymanager.impl.microservice;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.workflow.exposed.build.WorkflowConfiguration;

public interface WorkflowProxy {

    ApplicationId submitWorkflow(WorkflowConfiguration config);
}
