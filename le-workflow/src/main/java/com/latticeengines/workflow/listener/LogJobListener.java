package com.latticeengines.workflow.listener;

import java.util.Date;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;

public class LogJobListener extends LEJobListener {

    // max length of the parameter value that will be logged
    private static final int MAX_PARAMETER_VALUE_LEN = 500;

    private static final Logger log = LoggerFactory.getLogger(LogJobListener.class);

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
            output.append("  " + entry.getKey() + "=" + StringUtils.truncate(
                    entry.getValue() == null ? null : entry.getValue().toString(), MAX_PARAMETER_VALUE_LEN) + "\n");
        }
        int stepSeq = 0;
        for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
            boolean skipped = ExitStatus.NOOP.equals(stepExecution.getExitStatus());
            Date start = stepExecution.getStartTime();
            Date end = stepExecution.getEndTime();
            Long duration = end.getTime() - start.getTime();
            String msg = String.format("Step [%02d] %s", stepSeq, stepExecution.getStepName());
            if (duration > 1000) {
                msg += String.format(" : %.2f sec", duration.doubleValue() / 1000.0);
            } else if (skipped) {
                msg += " : skipped";
            }
            output.append(String.format("%s\n", msg));
            stepSeq++;
        }
        log.info(output.toString());
    }

}
