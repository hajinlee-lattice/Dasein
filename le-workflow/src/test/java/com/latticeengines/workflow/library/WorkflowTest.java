package com.latticeengines.workflow.library;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Set;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.workflow.functionalframework.AnotherSuccessfulStep;
import com.latticeengines.workflow.functionalframework.FailableStep;
import com.latticeengines.workflow.functionalframework.RunAgainWhenCompleteStep;
import com.latticeengines.workflow.functionalframework.SuccessfulStep;
import com.latticeengines.workflow.functionalframework.WorkflowFunctionalTestNGBase;

public class WorkflowTest extends WorkflowFunctionalTestNGBase {

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobRegistry jobRegistry;

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private FailableStep failableStep;

    @Autowired
    private AnotherSuccessfulStep anotherSuccessfulStep;

    @Autowired
    private SuccessfulStep successfulStep;

    @Autowired
    private RunAgainWhenCompleteStep runAgainWhenCompleteStep;

    @Test(groups = "functional", enabled = true)
    public void testFailableWorkflow() throws Exception {
        Job modelWorkflow = jobRegistry.getJob("FailableWorkflow");
        JobParameters parms = new JobParametersBuilder().addLong("time", System.currentTimeMillis())
                .addString("testParameter", "testValue", false).toJobParameters();

        failableStep.setFail(true);
        JobExecution jobExecution = jobLauncher.run(modelWorkflow, parms);
        Set<String> stepNames = getStepNamesFromExecution(jobExecution);
        assertTrue(stepNames.contains(successfulStep.name()));
        assertTrue(stepNames.contains(failableStep.name()));
        assertFalse(stepNames.contains(anotherSuccessfulStep.name()));
        assertEquals(jobExecution.getStatus(), BatchStatus.FAILED);

        failableStep.setFail(false);
        Long restartedExecutionId = jobOperator.restart(jobExecution.getId());
        JobExecution restartedExecution = jobExplorer.getJobExecution(restartedExecutionId);
        Set<String> restartedStepNames = getStepNamesFromExecution(restartedExecution);
        assertFalse(restartedStepNames.contains(successfulStep.name()));
        assertTrue(restartedStepNames.contains(failableStep.name()));
        assertTrue(restartedStepNames.contains(anotherSuccessfulStep.name()));
        assertEquals(restartedExecution.getStatus(), BatchStatus.COMPLETED);
    }

    @Test(groups = "functional", enabled = true)
    public void testRunCompletedStepAgainWorkflow() throws Exception {
        Job modelWorkflow = jobRegistry.getJob("RunCompletedStepAgainWorkflow");
        JobParameters parms = new JobParametersBuilder().addLong("time", System.currentTimeMillis())
                .addString("testParameter", "testValue", false).toJobParameters();

        failableStep.setFail(true);
        JobExecution jobExecution = jobLauncher.run(modelWorkflow, parms);
        Set<String> stepNames = getStepNamesFromExecution(jobExecution);
        assertTrue(stepNames.contains(runAgainWhenCompleteStep.name()));
        assertTrue(stepNames.contains(successfulStep.name()));
        assertEquals(jobExecution.getStatus(), BatchStatus.FAILED);

        failableStep.setFail(false);
        Long restartedExecutionId = jobOperator.restart(jobExecution.getId());
        JobExecution restartedExecution = jobExplorer.getJobExecution(restartedExecutionId);
        Set<String> restartedStepNames = getStepNamesFromExecution(restartedExecution);
        assertTrue(restartedStepNames.contains(runAgainWhenCompleteStep.name()));
        assertFalse(restartedStepNames.contains(successfulStep.name()));
        assertEquals(restartedExecution.getStatus(), BatchStatus.COMPLETED);
    }

}
