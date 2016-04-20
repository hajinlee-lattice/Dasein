package com.latticeengines.workflow.functionalframework;

import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.listener.LEJobListener;

@Component
public class FailingListener extends LEJobListener {
    public static int calls = 0;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        calls++;
        throw new RuntimeException("Failure!");
    }
}
