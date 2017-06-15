package com.latticeengines.cdl.workflow.listeners;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataFeed.Status;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("calculateStatsListener")
public class CalculateStatsListener extends LEJobListener {

    private static final Log log = LogFactory.getLog(CalculateStatsListener.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        String datafeedName = job.getInputContextValue(WorkflowContextConstants.Inputs.DATAFEED_NAME);
        String statusStr = job.getInputContextValue(WorkflowContextConstants.Inputs.DATAFEED_STATUS);
        Status status = Status.valueOf(statusStr);
        String customerSpace = job.getTenant().getId();

        if (jobExecution.getStatus() == BatchStatus.FAILED) {
            log.info(String.format(
                    "Workflow failed. Update datafeed status for customer %s with datafeed name of %s and status of %s",
                    customerSpace, datafeedName, status));
            metadataProxy.updateDataFeedStatus(customerSpace, datafeedName, status.getName());
        } else if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info(String.format(
                    "Workflow completed. Update datafeed status for customer %s with datafeed name of %s and status of %s",
                    customerSpace, datafeedName, Status.Active.getName()));
            metadataProxy.updateDataFeedStatus(customerSpace, datafeedName, Status.Active.getName());
        } else {
            log.warn("Workflow ended in an unknown state.");
        }
    }

}
