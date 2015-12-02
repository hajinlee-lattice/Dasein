package com.latticeengines.pls.entitymanager.impl.microservice;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;

public interface WorkflowProxy {

    ApplicationId submitWorkflow(WorkflowConfiguration config);

    WorkflowStatus getWorkflowStatusFromApplicationId(String applicationId);
}
