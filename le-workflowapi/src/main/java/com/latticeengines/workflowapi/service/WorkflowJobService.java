package com.latticeengines.workflowapi.service;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;

public interface WorkflowJobService {
    WorkflowExecutionId getWorkflowExecutionIdByApplicationId(String applicationId);

    WorkflowStatus getWorkflowStatus(Long workflowId);

    JobStatus getJobStatus(Long workflowId);

    List<JobStatus> getJobStatus(List<Long> workflowIds);

    List<JobStatus> getJobStatus(String customerSpace, List<Long> workflowIds);

    JobStatus getJobStatusByApplicationId(String applicationId);

    Job getJob(Long workflowId);

    Job getJob(Long workflowId, Boolean includeDetails);

    Job getJobByApplicationId(String applicationId);

    Job getJobByApplicationId(String applicationId, Boolean includeDetails);

    List<Job> getJobs(List<Long> workflowIds);

    List<Job> getJobs(List<Long> workflowIds, String type);

    List<Job> getJobs(List<Long> workflowIds, Boolean includeDetails);

    List<Job> getJobs(List<Long> workflowIds, List<String> types, Boolean includeDetails);

    List<Job> getJobs(String customerSpace, List<Long> workflowIds, List<String> types, Boolean includeDetails,
                      Boolean hasParentId, Long parentJobId);

    List<Job> getJobsByTenantPid(Long tenantPid);

    List<Job> getJobsByTenantPid(Long tenantPid, Boolean includeDetails);

    List<Job> getJobsByTenantPid(Long tenantPid, List<String> types, Boolean includeDetails);

    List<String> getStepNames(Long workflowId);

    List<Job> updateParentJobId(String customerSpace, List<Long> workflowIds, Long parentJobId);

    ApplicationId submitWorkFlow(WorkflowConfiguration workflowConfiguration);

    String submitAwsWorkflow(WorkflowConfiguration workflowConfiguration);

    void stopWorkflow(String customerSpace, Long workflowId);
}
