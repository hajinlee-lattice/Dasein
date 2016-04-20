package com.latticeengines.workflow.listener;

import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;

public class LogJobListener extends LEJobListener {

    private static final Log log = LogFactory.getLog(LogJobListener.class);

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        StringBuilder output = new StringBuilder();
        output.append("\nResults for " + jobExecution.getJobInstance().getJobName() + " \n");
        output.append("  StartTime     : " + jobExecution.getStartTime() + "\n");
        output.append("  EndTime    : " + jobExecution.getEndTime() + "\n");
        output.append("  ExitCode   : " + jobExecution.getExitStatus().getExitCode() + "\n");
        output.append("  ExitDescription : " + jobExecution.getExitStatus().getExitDescription() + "\n");
        output.append("  Status      : " + jobExecution.getStatus() + "\n");
        output.append("JobParameters: \n");
        JobParameters jobParameters = jobExecution.getJobParameters();
        for (Entry<String, JobParameter> entry : jobParameters.getParameters().entrySet()) {
            output.append("  " + entry.getKey() + "=" + entry.getValue() + "\n");
        }
        for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
            output.append("Step " + stepExecution.getStepName() + " \n");
        }
        log.info(output.toString());
    }

}
