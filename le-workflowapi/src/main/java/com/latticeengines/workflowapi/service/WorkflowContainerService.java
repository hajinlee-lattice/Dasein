package com.latticeengines.workflowapi.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.workflow.WorkflowId;
import com.latticeengines.workflow.exposed.build.WorkflowConfiguration;

public interface WorkflowContainerService {

    ApplicationId submitWorkFlow(WorkflowConfiguration workflowConfig);

    WorkflowId getWorkflowId(ApplicationId appId);
}
