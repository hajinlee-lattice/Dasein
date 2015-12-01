package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertEquals;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.prospectdiscovery.workflow.FitModelWorkflow;
import com.latticeengines.prospectdiscovery.workflow.FitModelWorkflowConfiguration;

public class FitModelWorkflowDeploymentTestNG extends FitModelWorkflowTestNGBase {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(FitModelWorkflowDeploymentTestNG.class);

    @Autowired
    private FitModelWorkflow fitModelWorkflow;

    @BeforeTest(groups = { "deployment" })
    public void setup() throws Exception {
        setupForFitModel();
    }

    @Test(groups = "deployment", enabled = true)
    public void testWorkflow() throws Exception {
        FitModelWorkflowConfiguration workflowConfig = generateFitModelWorkflowConfiguration();

        WorkflowExecutionId workflowId = workflowService.start(fitModelWorkflow.name(), workflowConfig);

        BatchStatus status = workflowService.waitForCompletion(workflowId, WORKFLOW_WAIT_TIME_IN_MILLIS).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
    }

    @Test(groups = "deployment", enabled = true)
    public void testWorkflowInContainer() throws Exception {
        FitModelWorkflowConfiguration workflowConfig = generateFitModelWorkflowConfiguration();

        workflowConfig.setContainerConfiguration(fitModelWorkflow.name(), DEMO_CUSTOMERSPACE,
                "FitModelWorkflowTest_submitWorkflow");

        submitWorkflowAndAssertSuccessfulCompletion(workflowConfig);
    }
}
