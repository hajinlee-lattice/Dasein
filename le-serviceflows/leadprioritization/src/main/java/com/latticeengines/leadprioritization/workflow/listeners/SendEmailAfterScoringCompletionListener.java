package com.latticeengines.leadprioritization.workflow.listeners;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.AdditionalEmailInfo;
import com.latticeengines.serviceflows.workflow.scoring.ScoreStepConfiguration;
import com.latticeengines.workflow.listener.LEJobListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;

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
        String hostPort = jobExecution.getJobParameters().getString("Internal_Resource_Host_Port");
        log.info("hostPort: " + hostPort);
        String userId = jobExecution.getJobParameters().getString("User_Id");
        AdditionalEmailInfo emailInfo = new AdditionalEmailInfo();
        emailInfo.setUserId(userId);
        String scoreStepConfiguration = jobExecution.getJobParameters().getString(
                ScoreStepConfiguration.class.getCanonicalName());
        String modelId = JsonUtils.deserialize(scoreStepConfiguration, ScoreStepConfiguration.class)
                .getModelId();
        emailInfo.setModelId(modelId);
        log.info(String.format("userId: %s; modelName: %s", emailInfo.getUserId(), emailInfo.getModelId()));
        InternalResourceRestApiProxy proxy = new InternalResourceRestApiProxy(hostPort);
        proxy.sendPlsScoreEmail(jobExecution.getStatus().name(), tenantId, emailInfo);

    }

}
