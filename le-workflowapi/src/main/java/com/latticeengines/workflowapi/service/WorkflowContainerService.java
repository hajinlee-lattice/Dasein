package com.latticeengines.workflowapi.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public interface WorkflowContainerService {

    ApplicationId submitWorkFlow(WorkflowConfiguration workflowConfig);

    String submitAwsWorkFlow(WorkflowConfiguration workflowConfig);
}
