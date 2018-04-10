package com.latticeengines.cdl.workflow.listeners;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("activateRatingEngineListener")
public class ActivateRatingEngineListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(ActivateRatingEngineListener.class);

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        String tenantId = jobExecution.getJobParameters().getString("CustomerSpace");
        log.info("tenantid: " + tenantId);
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        if (job != null) {
            String ratingEngineId = job.getInputContextValue(WorkflowContextConstants.Inputs.RATING_ENGINE_ID);
            if (StringUtils.isNotBlank(ratingEngineId)) {
                log.info("Found rating engine id in workflow input, activating the engine");
                ratingEngineProxy.activateRatingEngine(tenantId, ratingEngineId);
            }
        }
    }

}
