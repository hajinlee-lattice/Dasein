package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertEquals;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.prospectdiscovery.workflow.FitModelWorkflow;
import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.FitModelWorkflowConfiguration;

public class FitModelWorkflowDeploymentTestNG extends FitModelWorkflowTestNGBase {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(FitModelWorkflowDeploymentTestNG.class);

    @Autowired
    private FitModelWorkflow fitModelWorkflow;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setupForFitModel();
    }

    @Test(groups = "deployment", enabled = false)
    public void testWorkflow() throws Exception {
        FitModelWorkflowConfiguration workflowConfig = generateFitModelWorkflowConfiguration();

        WorkflowExecutionId workflowId = workflowService.start(fitModelWorkflow.name(), workflowConfig);

        // Line below is example of how to restart a workflow from the last failed step; also need to disable the setup
        //WorkflowExecutionId workflowId = workflowService.restart(new WorkflowExecutionId(18L));
        System.out.println("Workflow id = " + workflowId.getId());
        BatchStatus status = workflowService.waitForCompletion(workflowId, WORKFLOW_WAIT_TIME_IN_MILLIS).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
    }

}
