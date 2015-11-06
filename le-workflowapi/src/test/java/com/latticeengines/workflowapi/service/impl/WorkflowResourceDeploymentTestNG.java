package com.latticeengines.workflowapi.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.rest.URLUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflowapi.flows.ModelWorkflow;
import com.latticeengines.workflowapi.flows.ModelWorkflowConfiguration;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;
import com.latticeengines.workflowapi.steps.dlorchestration.ModelLoadDataConfiguration;

public class WorkflowResourceDeploymentTestNG extends WorkflowApiFunctionalTestNGBase {

    @Autowired
    private ModelWorkflow modelWorkflow;

    @Autowired
    private WorkflowService workflowService;

    @Test(groups = "deployment", enabled = true)
    public void submitWorkflow() throws Exception {
        ModelLoadDataConfiguration loadDataConfig = new ModelLoadDataConfiguration();
        loadDataConfig.setI(77);

        ModelWorkflowConfiguration workflowConfig = new ModelWorkflowConfiguration.Builder()
                .setModelLoadDataConfiguration(loadDataConfig).build();
        workflowConfig.setContainerConfiguration(modelWorkflow.name(), CUSTOMERSPACE,
                "WorkflowResourceDeploymentTest_submitWorkflow");

        AppSubmission submission = submitWorkflow(workflowConfig);
        assertNotNull(submission);
        assertNotEquals(submission.getApplicationIds().size(), 0);
        String appId = submission.getApplicationIds().get(0);
        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        String url = String.format("%s/workflowapi/workflows/yarnapps/%s", URLUtils.getRestAPIHostPort(hostPort), appId);
        String workflowId = restTemplate.getForObject(url, String.class);

        url = String.format("%s/workflowapi/workflows/%s", URLUtils.getRestAPIHostPort(hostPort), workflowId);
        WorkflowStatus workflowStatus = restTemplate.getForObject(url, WorkflowStatus.class);
        assertEquals(workflowStatus.getStatus(), BatchStatus.COMPLETED);
    }

}
