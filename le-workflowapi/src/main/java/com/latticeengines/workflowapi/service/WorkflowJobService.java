package com.latticeengines.workflowapi.service;

import java.util.List;

import org.springframework.batch.core.JobExecution;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;

public interface WorkflowJobService {
    JobStatus getJobStatus(Long workflowId);

    List<JobStatus> getJobStatus(List<Long> workflowIds);

    List<JobStatus> getJobStatus(String customerSpace, List<Long> workflowIds);

    Job getJob(Long workflowId);

    List<Job> getJobs(List<Long> workflowIds);

    List<Job> getJobs(List<Long> workflowIds, String type);

    List<Job> getJobs(String customerSpace, List<Long> workflowIds, List<String> types, Boolean includeDetails,
                      Boolean hasParentId, Long parentJobId);

    List<Job> getJobsByTenant(Long tenantPid);

    List<Job> getJobsByTenant(Long tenantPid, boolean included, List<String> types);

    @Deprecated
    JobStatus getJobStatusByApplicationId(String applicationId);

    List<String> getStepNames(WorkflowExecutionId workflowId);

    String getWorkflowName(JobExecution jobExecution);
}
