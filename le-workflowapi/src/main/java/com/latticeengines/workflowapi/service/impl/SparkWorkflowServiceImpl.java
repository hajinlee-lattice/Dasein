package com.latticeengines.workflowapi.service.impl;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.workflow.annotation.WithCustomerSpace;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.core.spark.RunSparkWorkflowConfig;
import com.latticeengines.domain.exposed.workflow.RunSparkWorkflowRequest;
import com.latticeengines.workflowapi.service.SparkWorkflowService;
import com.latticeengines.workflowapi.service.WorkflowJobService;

@Service
public class SparkWorkflowServiceImpl implements SparkWorkflowService {

    @Inject
    private WorkflowJobService workflowJobService;

    @Override
    @WithCustomerSpace
    public AppSubmission submitSparkJob(String customerSpace, RunSparkWorkflowRequest request) {
        RunSparkWorkflowConfig workflowConfig = new RunSparkWorkflowConfig.Builder() //
                .customer(CustomerSpace.parse(customerSpace)).request(request).build();
        ApplicationId applicationId = workflowJobService.submitWorkflow(customerSpace, workflowConfig, null);
        return new AppSubmission(applicationId);
    }

}
