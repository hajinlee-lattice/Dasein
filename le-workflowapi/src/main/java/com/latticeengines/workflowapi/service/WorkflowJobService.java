package com.latticeengines.workflowapi.service;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;

public interface WorkflowJobService {
    WorkflowExecutionId getWorkflowExecutionIdByApplicationId(String customerSpace, String applicationId);

    JobStatus getJobStatusByWorkflowId(String customerSpace, Long workflowId);

    JobStatus getJobStatusByWorkflowPid(String customerSpace, Long workflowPid);

    List<JobStatus> getJobStatusByWorkflowIds(String customerSpace, List<Long> workflowIds);

    List<JobStatus> getJobStatusByWorkflowPids(String customerSpace, List<Long> workflowPids);

    Job getJobByWorkflowId(String customerSpace, Long workflowId, Boolean includeDetails);

    Job getJobByWorkflowPid(String customerSpace, Long workflowPid, Boolean includeDetails);

    Job getJobByApplicationId(String customerSpace, String applicationId, Boolean includeDetails);

    List<Job> getJobsByCustomerSpace(String customerSpace, Boolean includeDetails);

    List<Job> getJobsByWorkflowIds(String customerSpace, List<Long> workflowIds, List<String> types,
                                   Boolean includeDetails, Boolean hasParentId, Long parentJobId);

    List<Job> getJobsByWorkflowPids(String customerSpace, List<Long> workflowIds, List<String> types,
                                   Boolean includeDetails, Boolean hasParentId, Long parentJobId);

    List<String> getStepNames(String customerSpace, Long workflowPid);

    void updateParentJobIdByWorkflowIds(String customerSpace, List<Long> workflowIds, Long parentJobId);

    void updateParentJobIdByWorkflowPids(String customerSpace, List<Long> workflowPids, Long parentJobId);

    ApplicationId submitWorkflow(String customerSpace, WorkflowConfiguration workflowConfiguration, Long workflowPid);

    String submitAwsWorkflow(String customerSpace, WorkflowConfiguration workflowConfiguration);

    Long createWorkflowJob(String customerSpace);

    void stopWorkflow(String customerSpace, Long workflowId);

    void stopWorkflowJob(String customerSpace, Long workflowPid);
}
