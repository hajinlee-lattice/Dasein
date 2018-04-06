package com.latticeengines.cdl.workflow.listeners;

import java.util.Collections;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("maintenanceOperationListener")
public class MaintenanceOperationListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceOperationListener.class);

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Inject
    private ActionProxy actionProxy;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
        log.info(String.format("Update job %s", jobExecution.getId()));
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        updateMaintenanceAction(job);
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        String customerSpace = job.getTenant().getId();
        String status = job.getInputContextValue(WorkflowContextConstants.Inputs.DATAFEED_STATUS);
        if (StringUtils.isEmpty(status)) {
            throw new RuntimeException("Cannot get initial data feed status for customer: " + customerSpace);
        }
        if (jobExecution.getStatus().isUnsuccessful()) {
            // reset data feed status.
            log.info(String.format("Maintenance workflow failed. Update datafeed status for customer %s with status %s",
                    customerSpace, status));
            DataFeedExecution dataFeedExecution = dataFeedProxy.failExecution(customerSpace, status);
            if (dataFeedExecution.getStatus() != DataFeedExecution.Status.Failed) {
                throw new RuntimeException("Cannot fail execution!");
            }

        } else if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            // check data feed status? should be Initing or InitialLoaded?
            DataFeedExecution dataFeedExecution = dataFeedProxy.finishExecution(customerSpace, status);
            if (dataFeedExecution.getStatus() != DataFeedExecution.Status.Completed) {
                throw new RuntimeException("Cannot finish execution!");
            }
        } else {
            log.error(String.format("CDL maintenance job ends in unknown status: %s", jobExecution.getStatus().name()));
        }

        dataFeedProxy.updateDataFeedMaintenanceMode(customerSpace, false);
    }

    private void updateMaintenanceAction(WorkflowJob job) {
        String ActionPidStr = job.getInputContextValue(WorkflowContextConstants.Inputs.ACTION_ID);
        if (ActionPidStr != null) {
            Long pid = Long.parseLong(ActionPidStr);
            log.info(String.format("Updating an actionPid=%d for job=%d", pid, job.getWorkflowId()));
            Action action = actionProxy.getActionsByPids(job.getTenant().getId(), Collections.singletonList(pid))
                    .get(0);
            if (action != null) {
                log.info(String.format("Action=%s", action));
                action.setTrackingId(job.getWorkflowId());
                actionProxy.updateAction(job.getTenant().getId(), action);
            } else {
                log.warn(String.format("Action with pid=%d cannot be found", pid));
            }
        } else {
            log.warn(String.format("ActionPid is not correctly registered by workflow job=%d", job.getWorkflowId()));
        }
    }
}
