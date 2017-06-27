package com.latticeengines.pls.service;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;

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

    Job find(String jobId);

    List<Job> findAll();

}
