package com.latticeengines.workflowapi.service;

import java.util.List;
import java.util.Set;

import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public interface WorkflowJobService {
    JobStatus getJobStatus(Long workflowId);

    List<JobStatus> getJobStatus(List<Long> workflowIds);

    List<JobStatus> getJobStatus(String customerSpace, List<Long> workflowIds);

    Job getJob(Long workflowId);

    Job getJob(Long workflowId, Boolean includeDetails);

    List<Job> getJobs(List<Long> workflowIds);

    List<Job> getJobs(List<Long> workflowIds, String type);

    List<Job> getJobs(List<Long> workflowIds, String type, Boolean includeDetails);

    List<Job> getJobs(List<Long> workflowIds, Boolean includeDetails);

    List<Job> getJobs(String customerSpace, List<Long> workflowIds, Set<String> types, Boolean includeDetails,
                      Boolean hasParentId, Long parentJobId);

    List<Job> getJobsByTenantPid(Long tenantPid);

    List<Job> getJobsByTenantPid(Long tenantPid, Boolean includeDetails);

    @Deprecated
    JobStatus getJobStatusByApplicationId(String applicationId);

    List<String> getStepNames(Long workflowId);

    void updateParentJobId(String customerSpace, List<Long> workflowIds, Long parentJobId);
}
