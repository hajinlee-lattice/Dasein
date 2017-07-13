package com.latticeengines.workflow.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

public abstract class LEJobListener implements JobExecutionListener {
    private static final Logger log = LoggerFactory.getLogger(LEJobListener.class);

    public abstract void beforeJobExecution(JobExecution jobExecution);

    public abstract void afterJobExecution(JobExecution jobExecution);

    @Override
    public final void beforeJob(JobExecution jobExecution) {
        try {
            beforeJobExecution(jobExecution);
        } catch (Exception e) {
            log.error(String.format("Caught error in job listener %s: %s", getClass().getName(), e.getMessage()), e);
        }
    }

    @Override
    public final void afterJob(JobExecution jobExecution) {
        try {
            afterJobExecution(jobExecution);
        } catch (Exception e) {
            log.error(String.format("Caught error in job listener %s: %s", getClass().getName(), e.getMessage()), e);
        }
    }
}
