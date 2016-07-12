package com.latticeengines.leadprioritization.workflow.listeners;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("SendEmailAfterRTSBulkScoringCompletionListener")
public class SendEmailAfterRTSBulkScoringCompletionListener extends LEJobListener {

    private static final Log log = LogFactory.getLog(JobExecutionListener.class);

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        String tenantId = jobExecution.getJobParameters().getString("CustomerSpace");
        log.info("tenantid: " + tenantId);
        String hostPort = jobExecution.getJobParameters().getString("Internal_Resource_Host_Port");
        log.info("hostPort: " + hostPort);
        InternalResourceRestApiProxy proxy = new InternalResourceRestApiProxy(hostPort);
        proxy.sendPlsScoreEmail(jobExecution.getStatus().name(), tenantId);

    }

}
