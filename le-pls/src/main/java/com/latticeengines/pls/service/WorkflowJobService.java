package com.latticeengines.pls.service;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public interface WorkflowJobService {

    JobStatus getJobStatusFromApplicationId(String appId);

    ApplicationId submit(WorkflowConfiguration configuration);

    ApplicationId restart(Long jobId);

    void cancel(String jobId);

    List<Job> findAllWithType(String type);

    Job findByApplicationId(String applicationId);

    Job find(String jobId, boolean useCustomerSpace);

    List<Job> findByJobIds(List<String> jobIds);

    List<Job> findJobs(List<String> jobIds, List<String> types, Boolean includeDetails, Boolean hasParentId);

    List<Job> findJobsBasedOnActionIdsAndType(List<Long> actionPids, ActionType actionType);

    List<Job> findAll();

    String generateCSVReport(String jobId);

}
