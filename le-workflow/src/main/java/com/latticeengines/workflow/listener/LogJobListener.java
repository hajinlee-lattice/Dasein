package com.latticeengines.workflow.listener;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;

public class LogJobListener extends LEJobListener {

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
            output.append("  " + entry.getKey() + "=" + entry.getValue() + "\n");
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat("MM-dd HH:mm:ss");
        for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
            Date start = stepExecution.getStartTime();
            Date end = stepExecution.getEndTime();
            Long duration = end.getTime() - start.getTime();
            output.append(String.format("Step %s : %s to %s ( %.2f sec )", stepExecution.getStepName(),
                    dateFormat.format(start), dateFormat.format(end), duration.doubleValue() / 1000.0));
        }
        log.info(output.toString());
    }

}
