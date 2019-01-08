package com.latticeengines.yarn.exposed.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;

public interface AwsBatchJobService {

    ApplicationId submitAwsBatchJob(Job job);

    JobStatus getAwsBatchJobStatus(String jobId);
}
