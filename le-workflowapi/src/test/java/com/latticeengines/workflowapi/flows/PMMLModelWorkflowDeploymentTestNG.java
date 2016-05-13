package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertEquals;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.leadprioritization.workflow.PMMLModelWorkflow;
import com.latticeengines.leadprioritization.workflow.PMMLModelWorkflowConfiguration;

public class PMMLModelWorkflowDeploymentTestNG extends PMMLModelWorkflowTestNGBase {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(PMMLModelWorkflowDeploymentTestNG.class);

    @Autowired
    private PMMLModelWorkflow pmmlModelWorkflow;
    
    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setupForPMMLModel();
    }

    @Test(groups = "deployment", enabled = true)
    public void testWorkflow() throws Exception {
        PMMLModelWorkflowConfiguration workflowConfig = generatePMMLModelWorkflowConfiguration();

        WorkflowExecutionId workflowId = workflowService.start(pmmlModelWorkflow.name(), workflowConfig);

        // Line below is example of how to restart a workflow from the last failed step; also need to disable the setup
        //WorkflowExecutionId workflowId = workflowService.restart(new WorkflowExecutionId(18L));
        System.out.println("Workflow id = " + workflowId.getId());
        BatchStatus status = workflowService.waitForCompletion(workflowId, WORKFLOW_WAIT_TIME_IN_MILLIS).getStatus();
        assertEquals(status, BatchStatus.FAILED);
    }

}
