package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertEquals;

import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.workflow.WorkflowId;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;
import com.latticeengines.workflowapi.steps.dlorchestration.ModelLoadDataConfiguration;

public class ModelWorkflowTestNG extends WorkflowApiFunctionalTestNGBase {

    @Autowired
    private ModelWorkflow modelWorkflow;

    @Test(groups = "functional", enabled = true)
    public void testWorkflow() throws Exception {
        ModelLoadDataConfiguration loadDataConfig = new ModelLoadDataConfiguration();
        loadDataConfig.setI(77);

        ModelWorkflowConfiguration workflowConfig = new ModelWorkflowConfiguration.Builder()
                .setModelLoadDataConfiguration(loadDataConfig).build();

        WorkflowId workflowId = workflowService.start(modelWorkflow.name(), workflowConfig);
        BatchStatus status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
    }

}
