package com.latticeengines.workflow.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.workflow.functionalframework.AnotherSuccessfulStep;
import com.latticeengines.workflow.functionalframework.FailableStep;
import com.latticeengines.workflow.functionalframework.FailableWorkflow;
import com.latticeengines.workflow.functionalframework.RunCompletedStepAgainWorkflow;
import com.latticeengines.workflow.functionalframework.SleepableStep;
import com.latticeengines.workflow.functionalframework.SleepableWorkflow;
import com.latticeengines.workflow.functionalframework.SuccessfulStep;
import com.latticeengines.workflow.functionalframework.WorkflowFunctionalTestNGBase;
import com.latticeengines.workflow.service.WorkflowId;
import com.latticeengines.workflow.service.WorkflowService;

public class WorkflowServiceImplTest extends WorkflowFunctionalTestNGBase {

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
        WorkflowId workflowId = workflowService.start(failableWorkflow.name());
        BatchStatus status = workflowService.getStatus(workflowId);
        assertEquals(status, BatchStatus.COMPLETED);
    }

    @Test(groups = "functional", enabled = true)
    public void testRestart() throws Exception {
        failableStep.setFail(true);
        WorkflowId workflowId = workflowService.start(failableWorkflow.name());
        List<String> stepNames = workflowService.getStepNames(workflowId);
        assertTrue(stepNames.contains(successfulStep.name()));
        assertTrue(stepNames.contains(failableStep.name()));
        assertFalse(stepNames.contains(anotherSuccessfulStep.name()));
        assertEquals(workflowService.getStatus(workflowId), BatchStatus.FAILED);

        failableStep.setFail(false);
        WorkflowId restartedWorkflowId = workflowService.restart(workflowId);
        List<String> restartedStepNames = workflowService.getStepNames(restartedWorkflowId);
        assertFalse(restartedStepNames.contains(successfulStep.name()));
        assertTrue(restartedStepNames.contains(failableStep.name()));
        assertTrue(restartedStepNames.contains(anotherSuccessfulStep.name()));
        assertEquals(workflowService.getStatus(restartedWorkflowId), BatchStatus.COMPLETED);
    }

    @Test(groups = "functional", enabled = true)
    public void testGetNames() throws Exception {
        List<String> workflowNames = workflowService.getNames();
        assertTrue(workflowNames.contains(failableWorkflow.name()));
        assertTrue(workflowNames.contains(runCompletedStepAgainWorkflow.name()));
    }

    // TODO enable when I can launch in separate thread either via different job launcher or in yarn container; then it's a deployment rather than functional test
    @Test(groups = "functional", enabled = false)
    public void testStop() throws Exception {
        sleepableStep.setSleepTime(100L);
        WorkflowId workflowId = workflowService.start(sleepableWorkflow.name());
        System.out.println(workflowService.getStatus(workflowId));
        workflowService.stop(workflowId);
        List<String> stepNames = workflowService.getStepNames(workflowId);

        assertTrue(stepNames.contains(successfulStep.name()));
        assertTrue(stepNames.contains(sleepableStep.name()));
        assertFalse(stepNames.contains(anotherSuccessfulStep.name()));
        System.out.println(workflowService.getStatus(workflowId));

        assertEquals(workflowService.getStatus(workflowId), BatchStatus.STOPPED);
    }
}
