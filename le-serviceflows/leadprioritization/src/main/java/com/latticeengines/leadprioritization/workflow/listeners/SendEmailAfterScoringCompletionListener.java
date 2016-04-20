package com.latticeengines.leadprioritization.workflow.listeners;

import com.latticeengines.workflow.listener.LEJobListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;
import com.latticeengines.serviceflows.workflow.scoring.ScoreStepConfiguration;

@Component("SendEmailAfterScoringCompletionListener")
public class SendEmailAfterScoringCompletionListener extends LEJobListener {

    private static final Log log = LogFactory.getLog(JobExecutionListener.class);

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        String tenantId = jobExecution.getJobParameters().getString("CustomerSpace");
        log.info("tenantid: " + tenantId);
        String scoreStepConfiguration = jobExecution.getJobParameters().getString(
                ScoreStepConfiguration.class.getCanonicalName());
        log.info("stepConf: " + scoreStepConfiguration);
        String hostPort = JsonUtils.deserialize(scoreStepConfiguration, ScoreStepConfiguration.class)
                .getInternalResourceHostPort();
        log.info("hostPort: " + hostPort);
        InternalResourceRestApiProxy proxy = new InternalResourceRestApiProxy(hostPort);
        proxy.sendPlsScoreEmail(jobExecution.getStatus().name(), tenantId);

    }

}
