package com.latticeengines.workflowapi.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;

public interface WorkflowContainerService {

    ApplicationId submitWorkFlow(WorkflowConfiguration workflowConfig);

    WorkflowExecutionId getWorkflowId(ApplicationId appId);
}
