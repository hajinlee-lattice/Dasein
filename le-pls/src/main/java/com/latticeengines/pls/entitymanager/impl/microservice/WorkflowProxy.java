package com.latticeengines.pls.entitymanager.impl.microservice;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public interface WorkflowProxy {

    ApplicationId submitWorkflow(WorkflowConfiguration config);
}
