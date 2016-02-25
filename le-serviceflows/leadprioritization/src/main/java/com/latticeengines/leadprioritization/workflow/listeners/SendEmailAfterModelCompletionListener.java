package com.latticeengines.leadprioritization.workflow.listeners;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;
import com.latticeengines.serviceflows.workflow.importdata.ImportStepConfiguration;

@Component("sendEmailAfterModelCompletionListener")
public class SendEmailAfterModelCompletionListener implements JobExecutionListener {

    private static final Log log = LogFactory.getLog(SendEmailAfterModelCompletionListener.class);

    @Override
    public void beforeJob(JobExecution jobExecution) {
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        String tenantId = jobExecution.getJobParameters().getString("CustomerSpace");
        log.info("tenantid: " + tenantId);
        String stepConfiguration = jobExecution.getJobParameters().getString(ImportStepConfiguration.class.getCanonicalName());
        log.info("stepConf: " + stepConfiguration);
        String hostPort = JsonUtils.deserialize(stepConfiguration, ImportStepConfiguration.class).getInternalResourceHostPort();
        log.info("hostPort: " + hostPort);
        InternalResourceRestApiProxy proxy = new InternalResourceRestApiProxy(hostPort);
        proxy.sendPlsCreateModelEmail(jobExecution.getStatus().name(), tenantId);
    }

}
