package com.latticeengines.cdl.workflow.listeners;

import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.listener.LEJobListener;

@Component("maintenanceOperationListener")
public class MaintenanceOperationListener extends LEJobListener {
    @Override
    public void beforeJobExecution(JobExecution jobExecution) {

    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {

    }
}
