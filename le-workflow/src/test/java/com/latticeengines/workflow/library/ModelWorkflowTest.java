package com.latticeengines.workflow.library;

import static org.testng.Assert.assertEquals;

import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.workflow.functionalframework.WorkflowFunctionalTestNGBase;
import com.latticeengines.workflow.service.WorkflowId;
import com.latticeengines.workflow.service.WorkflowService;

public class ModelWorkflowTest extends WorkflowFunctionalTestNGBase {

    @Autowired
    private WorkflowService workflowService;

    @Autowired
    private ModelWorkflow modelWorkflow;

    @Test(groups = "functional", enabled = true)
    public void testWorkflow() throws Exception {
        WorkflowId workflowId = workflowService.start(modelWorkflow.name());
        BatchStatus status = workflowService.getStatus(workflowId);
        assertEquals(status, BatchStatus.COMPLETED);
    }

}
