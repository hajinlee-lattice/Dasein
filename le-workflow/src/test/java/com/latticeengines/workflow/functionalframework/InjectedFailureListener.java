package com.latticeengines.workflow.functionalframework;

import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.listener.LEJobListener;


@Component
public class InjectedFailureListener extends LEJobListener {

    public String exitDescription;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        exitDescription = jobExecution.getExitStatus().getExitDescription();
    }
}
