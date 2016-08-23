package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.workflow.Job;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

import java.util.List;

public interface WorkflowJobService {

    JobStatus getJobStatusFromApplicationId(String appId);

    ApplicationId submit(WorkflowConfiguration configuration);

    AppSubmission restart(String jobId);

    void cancel(String jobId);

    List<Job> findAllWithType(String type);

    Job findByApplicationId(String applicationId);

    Job find(String jobId);

    List<Job> findAll();

}
