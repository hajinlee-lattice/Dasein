package com.latticeengines.workflow.exposed.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowInstanceId;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;

public interface WorkflowService {

    List<String> getNames();

    WorkflowExecutionId start(String workflowName, WorkflowConfiguration workflowConfiguration);

    WorkflowExecutionId start(String workflowName, String applicationId, WorkflowConfiguration workflowConfiguration);

    WorkflowExecutionId restart(WorkflowExecutionId workflowId);

    WorkflowExecutionId restart(WorkflowInstanceId workflowId);

    void stop(WorkflowExecutionId workflowId);

    WorkflowStatus getStatus(WorkflowExecutionId workflowId);

    List<String> getStepNames(WorkflowExecutionId workflowId);

    WorkflowStatus waitForCompletion(WorkflowExecutionId workflowId) throws Exception;

    WorkflowStatus waitForCompletion(WorkflowExecutionId workflowId, long maxWaitTime) throws Exception;

    Job getJob(WorkflowExecutionId workflowId);

    Job getJob(String applicationId);

    List<Job> getJobsByTenant(long tenantPid);

    List<Job> getJobsByWorkflowIds(List<WorkflowExecutionId> workflowIds);

    List<Job> getJobsByTenant(long tenantPid, String type);

    List<Job> getJobs(List<WorkflowExecutionId> workflowIds, String type);

    long startWorkflowJob(String workflowName, WorkflowConfiguration workflowConfiguration);

    Map<String, String> getInputs(Map<String, String> inputContext);

}
