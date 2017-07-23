package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertEquals;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.FitModelWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;

@Deprecated
public class FitModelWorkflowDeploymentTestNG extends FitModelWorkflowTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(FitModelWorkflowDeploymentTestNG.class);

    @BeforeClass(groups = { "workflow" })
    public void setup() throws Exception {
        setupForFitModel();
    }

    @Test(groups = "workflow", enabled = false)
    public void testWorkflow() throws Exception {
        FitModelWorkflowConfiguration workflowConfig = generateFitModelWorkflowConfiguration();

        WorkflowExecutionId workflowId = workflowService.start(workflowConfig);

        // Line below is example of how to restart a workflow from the last
        // failed step; also need to disable the setup
        // WorkflowExecutionId workflowId = workflowService.restart(new
        // WorkflowExecutionId(18L));
        System.out.println("Workflow id = " + workflowId.getId());
        BatchStatus status = workflowService.waitForCompletion(workflowId, WORKFLOW_WAIT_TIME_IN_MILLIS).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
    }

}
