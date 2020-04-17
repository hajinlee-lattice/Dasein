package com.latticeengines.dcp.workflow.listeners;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.proxy.exposed.pls.PlsInternalProxy;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("sourceImportListener")
public class SourceImportListener extends LEJobListener {

    public static final Logger log = LoggerFactory.getLogger(SourceImportListener.class);

    @Inject
    private PlsInternalProxy plsInternalProxy;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {

    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        log.info("Finish Source Import!");
        sendEmail(jobExecution);
    }

    private void sendEmail(JobExecution jobExecution) {
        String tenantId = jobExecution.getJobParameters().getString("CustomerSpace");
        log.info("tenantId=" + tenantId);
        BatchStatus jobStatus = jobExecution.getStatus();

        if (jobStatus == BatchStatus.COMPLETED) {
            plsInternalProxy.sendUploadCompletedEmail();
        } else if (jobStatus == BatchStatus.FAILED) {
            plsInternalProxy.sendUploadCompletedEmail();
        }
    }
}
