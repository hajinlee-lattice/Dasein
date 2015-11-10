package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertEquals;

import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;
import com.latticeengines.workflowapi.steps.dlorchestration.ModelLoadDataConfiguration;

public class DLOrchestrationWorkflowTestNG extends WorkflowApiFunctionalTestNGBase {

    @Autowired
    private DLOrchestrationWorkflow dlOrchestrationWorkflow;

    @Test(groups = "functional", enabled = true)
    public void testWorkflow() throws Exception {
        ModelLoadDataConfiguration loadDataConfig = new ModelLoadDataConfiguration();
        loadDataConfig.setI(77);

        DLOrchestrationWorkflowConfiguration workflowConfig = new DLOrchestrationWorkflowConfiguration.Builder()
                .setModelLoadDataConfiguration(loadDataConfig).build();

        WorkflowExecutionId workflowId = workflowService.start(dlOrchestrationWorkflow.name(), workflowConfig);
        BatchStatus status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
    }

}
