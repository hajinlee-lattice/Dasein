package com.latticeengines.network.exposed.workflowapi;

import java.util.List;

import org.springframework.web.bind.annotation.PathVariable;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;

public interface WorkflowInterface {

    AppSubmission submitWorkflowExecution(WorkflowConfiguration config);

    AppSubmission restartWorkflowExecution(String workflowId);

    WorkflowExecutionId getWorkflowId(String applicationId);

    WorkflowStatus getWorkflowStatus(String workflowId);

    WorkflowStatus getWorkflowStatusFromApplicationId(String applicationId);

    Job getWorkflowJobFromApplicationId(String applicationId);

    Job getWorkflowExecution(String workflowId);

    List<Job> getWorkflowExecutionsForTenant(long tenantPid);

    List<Job> getWorkflowExecutionsForTenant(long tenantPid, String type);

    void stopWorkflow(String workflowId);
}
