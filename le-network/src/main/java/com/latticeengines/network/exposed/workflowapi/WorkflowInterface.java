package com.latticeengines.network.exposed.workflowapi;

import java.util.List;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;

public interface WorkflowInterface {
    AppSubmission submitWorkflowExecution(WorkflowConfiguration config, String ... params);

    AppSubmission restartWorkflowExecution(String workflowId, String ... params);

    String submitAWSWorkflowExecution(WorkflowConfiguration workflowConfig, String ... params);

    void stopWorkflowExecution(String workflowId, String ... params);

    WorkflowExecutionId getWorkflowId(String applicationId, String ... params);

    WorkflowStatus getWorkflowStatus(String workflowId, String ... params);

    Job getWorkflowJobFromApplicationId(String applicationId, String ... params);

    Job getWorkflowExecution(String workflowId, String ... params);

    List<Job> getWorkflowExecutionsByJobIds(List<String> jobIds, String ... params);

    @Deprecated
    List<Job> getWorkflowExecutionsForTenant(Long tenantPid, String ... params);

    List<Job> getJobs(List<String> jobIds, List<String> types, Boolean includeDetails, String ... params);

    void updateParentJobId(List<String> jobIds, String parentJobId, String ... params);
}
