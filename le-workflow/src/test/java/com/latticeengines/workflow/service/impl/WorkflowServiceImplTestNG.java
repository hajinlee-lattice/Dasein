package com.latticeengines.workflow.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflow.functionalframework.AnotherSuccessfulStep;
import com.latticeengines.workflow.functionalframework.FailableStep;
import com.latticeengines.workflow.functionalframework.FailableWorkflow;
import com.latticeengines.workflow.functionalframework.RunCompletedStepAgainWorkflow;
import com.latticeengines.workflow.functionalframework.SleepableStep;
import com.latticeengines.workflow.functionalframework.SleepableWorkflow;
import com.latticeengines.workflow.functionalframework.SuccessfulStep;
import com.latticeengines.workflow.functionalframework.WorkflowFunctionalTestNGBase;

public class WorkflowServiceImplTestNG extends WorkflowFunctionalTestNGBase {

    @Autowired
    private WorkflowService workflowService;

    @Autowired
    private FailableStep failableStep;

    @Autowired
    private FailableWorkflow failableWorkflow;

    @Autowired
    private RunCompletedStepAgainWorkflow runCompletedStepAgainWorkflow;

    @Autowired
    private SleepableStep sleepableStep;

    @Autowired
    private SleepableWorkflow sleepableWorkflow;

    @Autowired
    private AnotherSuccessfulStep anotherSuccessfulStep;

    @Autowired
    private SuccessfulStep successfulStep;


    @Test(groups = "functional", enabled = true)
    public void testStart() throws Exception {
        failableStep.setFail(false);
        WorkflowExecutionId workflowId = workflowService.start(failableWorkflow.name(), null);
        BatchStatus status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
    }

    @Test(groups = "functional", enabled = true)
    public void testRestart() throws Exception {
        failableStep.setFail(true);
        WorkflowExecutionId workflowId = workflowService.start(failableWorkflow.name(), null);
        BatchStatus status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT).getStatus();
        List<String> stepNames = workflowService.getStepNames(workflowId);
        assertTrue(stepNames.contains(successfulStep.name()));
        assertTrue(stepNames.contains(failableStep.name()));
        assertFalse(stepNames.contains(anotherSuccessfulStep.name()));
        assertEquals(status, BatchStatus.FAILED);

        failableStep.setFail(false);
        WorkflowExecutionId restartedWorkflowId = workflowService.restart(workflowId);
        status = workflowService.waitForCompletion(restartedWorkflowId, MAX_MILLIS_TO_WAIT).getStatus();
        List<String> restartedStepNames = workflowService.getStepNames(restartedWorkflowId);
        assertFalse(restartedStepNames.contains(successfulStep.name()));
        assertTrue(restartedStepNames.contains(failableStep.name()));
        assertTrue(restartedStepNames.contains(anotherSuccessfulStep.name()));
        assertEquals(status, BatchStatus.COMPLETED);
    }

    @Test(groups = "functional", enabled = true)
    public void testGetNames() throws Exception {
        List<String> workflowNames = workflowService.getNames();
        assertTrue(workflowNames.contains(failableWorkflow.name()));
        assertTrue(workflowNames.contains(runCompletedStepAgainWorkflow.name()));
    }

    @Test(groups = "functional", enabled = true)
    public void testStop() throws Exception {
        sleepableStep.setSleepTime(500L);
        WorkflowExecutionId workflowId = workflowService.start(sleepableWorkflow.name(), null);
        BatchStatus status = workflowService.getStatus(workflowId).getStatus();
        assertTrue(status.equals(BatchStatus.STARTING) || status.equals(BatchStatus.STARTED));

        Thread.sleep(400L);

        workflowService.stop(workflowId);
        List<String> stepNames = workflowService.getStepNames(workflowId);

        assertTrue(stepNames.contains(successfulStep.name()));
        assertTrue(stepNames.contains(sleepableStep.name()));
        assertFalse(stepNames.contains(anotherSuccessfulStep.name()));

        status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT).getStatus();
        assertEquals(status, BatchStatus.STOPPED);
    }
}
