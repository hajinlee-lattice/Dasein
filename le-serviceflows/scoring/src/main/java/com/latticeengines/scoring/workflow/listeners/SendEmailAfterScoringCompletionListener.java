package com.latticeengines.scoring.workflow.listeners;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.AdditionalEmailInfo;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.pls.EmailProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("SendEmailAfterScoringCompletionListener")
public class SendEmailAfterScoringCompletionListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(SendEmailAfterScoringCompletionListener.class);

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Inject
    private EmailProxy emailProxy;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        String tenantId = jobExecution.getJobParameters().getString("CustomerSpace");
        log.info("tenantid: " + tenantId);
        String userId = jobExecution.getJobParameters().getString("User_Id");
        AdditionalEmailInfo emailInfo = new AdditionalEmailInfo();
        emailInfo.setUserId(userId);
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        if (job != null) {
            String modelId = job.getInputContextValue(WorkflowContextConstants.Inputs.MODEL_ID);
            emailInfo.setModelId(modelId);
            log.info(String.format("userId: %s; modelId: %s", emailInfo.getUserId(), emailInfo.getModelId()));
            try {
                emailProxy.sendPlsScoreEmail(jobExecution.getStatus().name(), tenantId, emailInfo);
            } catch (Exception e) {
                log.error("Can not send scoring email: " + e.getMessage());
            }
        }
    }

}
