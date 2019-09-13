package com.latticeengines.workflowapi.service;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.item.ExecutionContext;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;

public interface WorkflowJobService {
    WorkflowExecutionId getWorkflowExecutionIdByApplicationId(String customerSpace, String applicationId);

    JobStatus getJobStatusByWorkflowId(String customerSpace, Long workflowId);

    JobStatus getJobStatusByWorkflowPid(String customerSpace, Long workflowPid);

    List<JobStatus> getJobStatusByWorkflowIds(String customerSpace, List<Long> workflowIds);

    List<JobStatus> getJobStatusByWorkflowPids(String customerSpace, List<Long> workflowPids);

    Job getJobByWorkflowId(String customerSpace, Long workflowId, Boolean includeDetails);

    Job getJobByWorkflowIdFromCache(String customerSpace, Long workflowId, boolean includeDetails);

    Job getJobByWorkflowPid(String customerSpace, Long workflowPid, Boolean includeDetails);

    Job getJobByApplicationId(String customerSpace, String applicationId, Boolean includeDetails);

    List<Job> getJobsByCustomerSpace(String customerSpace, Boolean includeDetails);

    List<Job> getJobsByCustomerSpaceFromCache(String customerSpace, Boolean includeDetails);

    List<Job> getJobsByWorkflowIds(String customerSpace, List<Long> workflowIds, List<String> types,
                                   Boolean includeDetails, Boolean hasParentId, Long parentJobId);

    List<Job> getJobsByWorkflowIds(String customerSpace, List<Long> workflowIds, List<String> types,
            List<String> jobStatuses, Boolean includeDetails, Boolean hasParentId, Long parentJobId);

    List<Job> getJobsByWorkflowIdsFromCache(String customerSpace, List<Long> workflowIds, boolean includeDetails);

    List<Job> getJobsByWorkflowPids(String customerSpace, List<Long> workflowPids, List<String> types,
                                   Boolean includeDetails, Boolean hasParentId, Long parentJobId);

    JobExecution getJobExecutionByWorkflowId(String customerSpace, Long workflowId);

    JobExecution getJobExecutionByWorkflowPid(String customerSpace, Long workflowPid);

    JobExecution getJobExecutionByApplicationId(String customerSpace, String applicationId);

    ExecutionContext getExecutionContextByWorkflowId(String customerSpace, Long workflowId);

    ExecutionContext getExecutionContextByWorkflowPid(String customerSpace, Long workflowPid);

    ExecutionContext getExecutionContextByApplicationId(String customerSpace, String applicationId);

    List<String> getStepNames(String customerSpace, Long workflowPid);

    void updateParentJobIdByWorkflowIds(String customerSpace, List<Long> workflowIds, Long parentJobId);

    void updateParentJobIdByWorkflowPids(String customerSpace, List<Long> workflowPids, Long parentJobId);

    /**
     * Update status of an already retried job to status {@link JobStatus#RETRIED}
     *
     * @param customerSpace
     *            customer tenant ID
     * @param workflowId
     *            retried job's workflow ID
     */
    void updateWorkflowStatusAfterRetry(String customerSpace, Long workflowId);

    ApplicationId submitWorkflow(String customerSpace, WorkflowConfiguration workflowConfiguration, Long workflowPid);

    ApplicationId enqueueWorkflow(String customerSpace, WorkflowConfiguration workflowConfiguration, Long workflowJobPid);

    List<ApplicationId> drainWorkflowQueue(String podid, String division);

    String submitAwsWorkflow(String customerSpace, WorkflowConfiguration workflowConfiguration);

    Long createWorkflowJob(String customerSpace);

    Long createFailedWorkflowJob(String customerSpace, Job failedJob);

    void stopWorkflow(String customerSpace, Long workflowId);

    void stopWorkflowJob(String customerSpace, Long workflowPid);

    void setErrorCategoryByJobPid(String customerSpace, Long workflowPid, String errorCategory);

    WorkflowJob deleteWorkflowJobByApplicationId(String customerSpace, String applicationId);

    List<WorkflowJob> deleteWorkflowJobs(String customerSpace, String type, Long startTime, Long endTime);

    void deleteByTenantPid(String customerSpace, Long tenantPid);

    /**
     * Clear all job cache entries for specified customer space.
     *
     * @param customerSpace
     *            target customer space, must belong to a valid tenant
     * @return number of job cache entries cleared
     */
    int clearJobCaches(@NotNull String customerSpace);

    /**
     * Clear all job cache entries that belong to specified workflow IDs and
     * customer space.
     *
     * @param customerSpace
     *            target customer space, must belong to a valid tenant
     * @param workflowIds
     *            list of workflowIds to clear
     * @return number of job cache entries cleared
     */
    int clearJobCachesByWorkflowIds(@NotNull String customerSpace, List<Long> workflowIds);

    /*
     * cross-tenant methods
     */

    /**
     * Retrieve the number of jobs in current cluster that are not in terminal
     * state. An optional list of job type filter can be applied.
     *
     * @param customerSpace
     *            current customer space
     * @param types
     *            list of job types for filtering, {@literal null} to retrieve any
     *            type
     * @return number of jobs that satisfy all conditions
     */
    int getNonTerminalJobCount(String customerSpace, List<String> types);

    List<WorkflowJob> queryByClusterIDAndTypesAndStatuses(String clusterId, List<String> workflowTypes, List<String> statuses);

    /**
     * Clear all job cache entries.
     *
     * @return number of job cache entries cleared
     */
    int clearAllJobCaches();

    void scheduledDrainQueueWrapper();
}
