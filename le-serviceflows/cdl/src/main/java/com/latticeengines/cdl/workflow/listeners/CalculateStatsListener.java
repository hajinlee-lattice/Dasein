package com.latticeengines.cdl.workflow.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("calculateStatsListener")
public class CalculateStatsListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(CalculateStatsListener.class);

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        String statusStr = job.getInputContextValue(WorkflowContextConstants.Inputs.DATAFEED_STATUS);
        Status status = Status.fromName(statusStr);
        String customerSpace = job.getTenant().getId();

        if (jobExecution.getStatus() == BatchStatus.FAILED) {
            log.info(String.format(
                    "Workflow failed. Update datafeed status for customer %s with status of %s",
                    customerSpace, status));
            dataFeedProxy.updateDataFeedStatus(customerSpace, status.getName());
        } else if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info(String.format(
                    "Workflow completed. Update datafeed status for customer %s with status of %s",
                    customerSpace, Status.Active.getName()));
            dataFeedProxy.updateDataFeedStatus(customerSpace, Status.Active.getName());
        } else {
            log.warn("Workflow ended in an unknown state.");
        }
    }

}
