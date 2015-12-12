package com.latticeengines.network.exposed.workflowapi;

import java.util.List;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;

public interface WorkflowInterface {

    AppSubmission submitWorkflowExecution(WorkflowConfiguration config);

    String getWorkflowId(String applicationId);

    WorkflowStatus getWorkflowStatus(String workflowId);

    WorkflowStatus getWorkflowStatusFromApplicationId(String applicationId);

    Job getWorkflowExecution(String workflowId);

    List<Job> getWorkflowExecutionsForTenant(long tenantPid);

    void stopWorkflow(String workflowId);
}
