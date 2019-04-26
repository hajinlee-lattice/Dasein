package com.latticeengines.cdl.workflow.listeners;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.workflow.listener.LEJobListener;

@Component
public class EntityExportWorkflowListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(EntityExportWorkflowListener.class);

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
        sendEmail(jobExecution);
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        sendEmail(jobExecution);
    }

    private void sendEmail(JobExecution jobExecution) {
        String tenantId = CustomerSpace.shortenCustomerSpace( //
                jobExecution.getJobParameters().getString("CustomerSpace"));
    }

}
