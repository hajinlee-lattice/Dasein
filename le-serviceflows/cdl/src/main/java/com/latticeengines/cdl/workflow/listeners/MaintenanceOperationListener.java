package com.latticeengines.cdl.workflow.listeners;

import java.util.Collections;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("maintenanceOperationListener")
public class MaintenanceOperationListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceOperationListener.class);

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceProxy;

    @PostConstruct
    public void init() {
        internalResourceProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        updateImportAction(job);
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        String customerSpace = job.getTenant().getId();
        String status = job.getOutputContextValue(WorkflowContextConstants.Outputs.DATAFEED_STATUS);
        if (jobExecution.getStatus().isUnsuccessful()) {
            if (!StringUtils.isEmpty(status)) {
                // reset data feed status.
                dataFeedProxy.updateDataFeedStatus(customerSpace, status);
            }
        } else if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            // check data feed status? should be Initing or InitialLoaded?
            dataFeedProxy.updateDataFeedStatus(customerSpace, DataFeed.Status.Active.getName());
        } else {
            log.error(String.format("CDL maintenance job ends in unknown status: %s", jobExecution.getStatus().name()));
        }

        dataFeedProxy.updateDataFeedMaintenanceMode(customerSpace, false);
    }

    private void updateImportAction(WorkflowJob job) {
        String ActionPidStr = job.getInputContextValue(WorkflowContextConstants.Inputs.ACTION_ID);
        if (ActionPidStr != null) {
            Long pid = Long.parseLong(ActionPidStr);
            log.info(String.format("Updating an actionPid=%d for job=%d", pid, job.getWorkflowId()));
            Action action = internalResourceProxy.findByPidIn(job.getTenant().getId(), Collections.singletonList(pid))
                    .get(0);
            if (action != null) {
                log.info(String.format("Action=%s", action));
                action.setTrackingId(job.getWorkflowId());
                internalResourceProxy.updateAction(job.getTenant().getId(), action);
            } else {
                log.warn(String.format("Action with pid=%d cannot be found", pid));
            }
        } else {
            log.warn(String.format("ActionPid is not correctly registered by workflow job=%d", job.getWorkflowId()));
        }
    }
}
