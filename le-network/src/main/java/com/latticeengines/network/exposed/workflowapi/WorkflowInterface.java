package com.latticeengines.network.exposed.workflowapi;

import java.util.List;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;

public interface WorkflowInterface {

    AppSubmission submitWorkflowExecution(WorkflowConfiguration config);

    AppSubmission restartWorkflowExecution(Long workflowId);

    WorkflowExecutionId getWorkflowId(String applicationId);

    WorkflowStatus getWorkflowStatus(String workflowId);

    Job getWorkflowJobFromApplicationId(String applicationId);

    Job getWorkflowExecution(String workflowId, boolean isCached);

    List<Job> getWorkflowExecutionsForTenant(long tenantPid);

    List<Job> getWorkflowExecutionsForTenant(long tenantPid, String type);

    void stopWorkflow(String workflowId);

    String submitAWSWorkflowExecution(WorkflowConfiguration workflowConfig);
}
