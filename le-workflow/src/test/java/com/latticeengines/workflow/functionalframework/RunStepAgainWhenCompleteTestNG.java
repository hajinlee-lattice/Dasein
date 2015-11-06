package com.latticeengines.workflow.functionalframework;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.workflow.WorkflowId;

public class RunStepAgainWhenCompleteTestNG extends WorkflowFunctionalTestNGBase {

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
        WorkflowId workflowId = workflowService.start(runCompletedStepAgainWorkflow.name(), null);
        BatchStatus status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT).getStatus();
        List<String> stepNames = workflowService.getStepNames(workflowId);
        assertTrue(stepNames.contains(runAgainWhenCompleteStep.name()));
        assertTrue(stepNames.contains(successfulStep.name()));
        assertEquals(status, BatchStatus.FAILED);

        failableStep.setFail(false);
        WorkflowId restartedWorkflowId = workflowService.restart(workflowId);
        status = workflowService.waitForCompletion(restartedWorkflowId, MAX_MILLIS_TO_WAIT).getStatus();
        List<String> restartedStepNames = workflowService.getStepNames(restartedWorkflowId);
        assertTrue(restartedStepNames.contains(runAgainWhenCompleteStep.name()));
        assertFalse(restartedStepNames.contains(successfulStep.name()));
        assertEquals(status, BatchStatus.COMPLETED);
    }

}
