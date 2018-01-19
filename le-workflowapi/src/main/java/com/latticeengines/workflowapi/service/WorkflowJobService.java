package com.latticeengines.workflowapi.service;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;

public interface WorkflowJobService {
    WorkflowExecutionId getWorkflowExecutionIdByApplicationId(String customerSpace, String applicationId);

    WorkflowStatus getWorkflowStatus(String customerSpace, Long workflowId);

    JobStatus getJobStatus(String customerSpace, Long workflowId);

    List<JobStatus> getJobStatus(String customerSpace, List<Long> workflowIds);

    Job getJob(String customerSpace, Long workflowId, Boolean includeDetails);

    Job getJobByApplicationId(String customerSpace, String applicationId, Boolean includeDetails);

    List<Job> getJobs(String customerSpace, Boolean includeDetails);

    List<Job> getJobs(String customerSpace, List<Long> workflowIds, List<String> types, Boolean includeDetails,
                      Boolean hasParentId, Long parentJobId);

    List<String> getStepNames(String customerSpace, Long workflowId);

    void updateParentJobId(String customerSpace, List<Long> workflowIds, Long parentJobId);

    ApplicationId submitWorkFlow(String customerSpace, WorkflowConfiguration workflowConfiguration);

    String submitAwsWorkflow(String customerSpace, WorkflowConfiguration workflowConfiguration);

    void stopWorkflow(String customerSpace, Long workflowId);
}
