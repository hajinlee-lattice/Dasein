package com.latticeengines.workflow.library;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.workflow.functionalframework.FailableStep;
import com.latticeengines.workflow.functionalframework.RunAgainWhenCompleteStep;
import com.latticeengines.workflow.functionalframework.RunCompletedStepAgainWorkflow;
import com.latticeengines.workflow.functionalframework.SuccessfulStep;
import com.latticeengines.workflow.functionalframework.WorkflowFunctionalTestNGBase;
import com.latticeengines.workflow.service.WorkflowId;
import com.latticeengines.workflow.service.WorkflowService;

public class RunStepAgainWhenCompleteTest extends WorkflowFunctionalTestNGBase {

    @Autowired
    private WorkflowService workflowService;

    @Autowired
    private FailableStep failableStep;

    @Autowired
    private SuccessfulStep successfulStep;

    @Autowired
    private RunAgainWhenCompleteStep runAgainWhenCompleteStep;

    @Autowired
    private RunCompletedStepAgainWorkflow runCompletedStepAgainWorkflow;

    @Test(groups = "functional", enabled = true)
    public void testRunCompletedStepAgainWorkflow() throws Exception {
        failableStep.setFail(true);
        WorkflowId workflowId = workflowService.start(runCompletedStepAgainWorkflow.name());
        List<String> stepNames = workflowService.getStepNames(workflowId);
        assertTrue(stepNames.contains(runAgainWhenCompleteStep.name()));
        assertTrue(stepNames.contains(successfulStep.name()));
        assertEquals(workflowService.getStatus(workflowId), BatchStatus.FAILED);

        failableStep.setFail(false);
        WorkflowId restartedWorkflowId = workflowService.restart(workflowId);
        List<String> restartedStepNames = workflowService.getStepNames(restartedWorkflowId);
        assertTrue(restartedStepNames.contains(runAgainWhenCompleteStep.name()));
        assertFalse(restartedStepNames.contains(successfulStep.name()));
        assertEquals(workflowService.getStatus(restartedWorkflowId), BatchStatus.COMPLETED);
    }

}
