package com.latticeengines.apps.core.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public interface WorkflowJobService {

    JobStatus getJobStatusFromApplicationId(String appId);

    ApplicationId submit(WorkflowConfiguration configuration);

    ApplicationId submit(WorkflowConfiguration configuration, Long workflowPid);

    ApplicationId restart(Long jobId, String customerSpace, Integer memory);

    ApplicationId restart(Long jobId, String customerSpace, Integer memory, Boolean autoRetry);

}
