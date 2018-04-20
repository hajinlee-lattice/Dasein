package com.latticeengines.modeling.workflow.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.AdditionalEmailInfo;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.workflow.exposed.build.InternalResourceRestApiProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("sendEmailAfterModelCompletionListener")
public class SendEmailAfterModelCompletionListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(SendEmailAfterModelCompletionListener.class);

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Autowired
    private RatingEngineProxy ratingEngineProxy;

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
            String modelName = job.getInputContextValue(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME);
            String ratingEngineId = job.getInputContextValue(WorkflowContextConstants.Inputs.RATING_ENGINE_ID);
            if (ratingEngineId != null) {
                RatingEngine ratingEngine = ratingEngineProxy.getRatingEngine(tenantId, ratingEngineId);
                emailInfo.setModelId(ratingEngine != null ? ratingEngine.getDisplayName() : modelName);
            } else {
                emailInfo.setModelId(modelName);
            }
            log.info(String.format("userId: %s; modelName: %s", emailInfo.getUserId(), emailInfo.getModelId()));
            InternalResourceRestApiProxy proxy = new InternalResourceRestApiProxy(hostPort);
            try {
                proxy.sendPlsCreateModelEmail(jobExecution.getStatus().name(), tenantId, emailInfo);
            } catch (Exception e) {
                log.error("Can not send create model email: " + e.getMessage());
            }
        }
    }

}
