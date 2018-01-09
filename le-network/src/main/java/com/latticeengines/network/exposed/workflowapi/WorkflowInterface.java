package com.latticeengines.network.exposed.workflowapi;

import java.util.List;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;

public interface WorkflowInterface {

    List<Job> getWorkflowJobs(String customerSpace, List<String> jobIds, List<String> types, Boolean includeDetails);

    List<Job> updateParentJobId(String customerSpace, List<String> jobIds, String parentJobId);

    AppSubmission submitWorkflowExecution(WorkflowConfiguration config);

    String submitAWSWorkflowExecution(WorkflowConfiguration workflowConfig);

    void stopWorkflow(String customerSpace, String workflowId);

    AppSubmission restartWorkflowExecution(Long workflowId);

    WorkflowExecutionId getWorkflowId(String applicationId);

    WorkflowStatus getWorkflowStatus(String workflowId);

    Job getWorkflowJobFromApplicationId(String applicationId);

    Job getWorkflowExecution(String workflowId);

    List<Job> getWorkflowExecutionsForTenant(Long tenantPid);

    List<Job> getWorkflowExecutionsByJobIds(List<String> jobIds);

    List<Job> getWorkflowExecutionsForTenant(long tenantPid, String type);
}
