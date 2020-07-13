package com.latticeengines.scoring.workflow.listeners;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.AdditionalEmailInfo;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.pls.EmailProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("SendEmailAfterRTSBulkScoringCompletionListener")
public class SendEmailAfterRTSBulkScoringCompletionListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(SendEmailAfterRTSBulkScoringCompletionListener.class);

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

                Object involvedEnrichmentForInternalAttributes = jobExecution.getExecutionContext()
                        .get(WorkflowContextConstants.Outputs.ENRICHMENT_FOR_INTERNAL_ATTRIBUTES_ATTEMPTED);
                if (involvedEnrichmentForInternalAttributes != null //
                        && involvedEnrichmentForInternalAttributes.toString()
                                .equalsIgnoreCase(Boolean.TRUE.toString())) {
                    String serializedInternalEnrichmentAttributes = (String) jobExecution.getExecutionContext()
                            .get(WorkflowContextConstants.Outputs.INTERNAL_ENRICHMENT_ATTRIBUTES_LIST);
                    List<?> internalEnrichmentAttributesObj = JsonUtils
                            .deserialize(serializedInternalEnrichmentAttributes, List.class);
                    List<String> internalEnrichmentAttributes = JsonUtils.convertList(internalEnrichmentAttributesObj,
                            String.class);
                    emailInfo.setExtraInfoList(internalEnrichmentAttributes);
                    emailProxy.sendPlsEnrichInternalAttributeEmail(jobExecution.getStatus().name(), tenantId, emailInfo);
                }
            } catch (Exception e) {
                log.error("Can not send RTS bulk scoring email: " + e.getMessage());
            }
        }
    }

}
