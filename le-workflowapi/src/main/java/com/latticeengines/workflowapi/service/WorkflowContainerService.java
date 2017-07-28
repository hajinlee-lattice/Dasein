package com.latticeengines.workflowapi.service;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;

public interface WorkflowContainerService {

    ApplicationId submitWorkFlow(WorkflowConfiguration workflowConfig);

    WorkflowExecutionId getWorkflowId(ApplicationId appId);

    Job getJobByApplicationId(String applicationId);

    List<Job> getJobsByTenant(long tenantPid, String type);

    List<Job> getJobsByTenant(long tenantPid);

    Job getJobFromWorkflowJobAndYarn(WorkflowJob workflowJob);

    String submitAwsWorkFlow(WorkflowConfiguration workflowConfig);
}
