package com.latticeengines.workflowapi.service;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.workflow.RunSparkWorkflowRequest;

public interface SparkWorkflowService {

    AppSubmission submitSparkJob(String customerSpace, RunSparkWorkflowRequest request);

}
