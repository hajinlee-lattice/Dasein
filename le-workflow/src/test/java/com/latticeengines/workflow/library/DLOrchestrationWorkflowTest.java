package com.latticeengines.workflow.library;

import static org.testng.Assert.assertEquals;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.workflow.functionalframework.WorkflowFunctionalTestNGBase;

public class DLOrchestrationWorkflowTest extends WorkflowFunctionalTestNGBase {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobRegistry jobRegistry;

    @Test(groups = "functional", enabled = true)
    public void testWorkflow() throws Exception {
        Job modelWorkflow = jobRegistry.getJob("ModelWorkflow");
        JobParameters parms = new JobParametersBuilder().addLong("time", System.currentTimeMillis())
                .addString("testParameter", "testValue", false).toJobParameters();
        JobExecution jobExecution = jobLauncher.run(modelWorkflow, parms);
        assertEquals(jobExecution.getStatus(), BatchStatus.COMPLETED);

        Job dlOrchestration = jobRegistry.getJob("DLOrchestrationWorkflow");
        parms = new JobParametersBuilder().addLong("time", System.currentTimeMillis())
                .addString("testParameter", "testValue", false).toJobParameters();
        jobExecution = jobLauncher.run(dlOrchestration, parms);
        assertEquals(jobExecution.getStatus(), BatchStatus.COMPLETED);
    }

}
