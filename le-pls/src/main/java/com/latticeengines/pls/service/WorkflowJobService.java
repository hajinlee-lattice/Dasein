package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.workflow.JobStatus;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;

public interface WorkflowJobService {

    JobStatus getJobStatusFromApplicationId(String appId);

    ApplicationId submit(WorkflowConfiguration configuration);

}
