package com.latticeengines.leadprioritization.workflow.listeners;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.AdditionalEmailInfo;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("SendEmailAfterScoringCompletionListener")
public class SendEmailAfterScoringCompletionListener extends LEJobListener {

    private static final Log log = LogFactory.getLog(JobExecutionListener.class);

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

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
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        if (job != null) {
            String modelId = job.getInputContextValue(WorkflowContextConstants.Inputs.MODEL_ID);
            emailInfo.setModelId(modelId);
            log.info(String.format("userId: %s; modelId: %s", emailInfo.getUserId(), emailInfo.getModelId()));
            InternalResourceRestApiProxy proxy = new InternalResourceRestApiProxy(hostPort);
            try {
                proxy.sendPlsScoreEmail(jobExecution.getStatus().name(), tenantId, emailInfo);
            } catch (Exception e) {
                log.error("Can not send scoring email: " + e.getMessage());
            }
        }
    }

}
