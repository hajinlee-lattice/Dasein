package com.latticeengines.pls.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public interface WorkflowJobService {

    JobStatus getJobStatusFromApplicationId(String appId);

    ApplicationId submit(WorkflowConfiguration configuration);

}
