package com.latticeengines.workflowapi.service;

import com.latticeengines.domain.exposed.workflowapi.WorkflowLogLinks;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public interface WorkflowContainerService {

    ApplicationId submitWorkflow(WorkflowConfiguration workflowConfig, Long workflowPid);

    String submitAwsWorkflow(WorkflowConfiguration workflowConfig, Long workflowPid);

    JobStatus getJobStatus(String applicationId);

    WorkflowLogLinks getLogUrlByWorkflowPid(Long workflowPid);
}
