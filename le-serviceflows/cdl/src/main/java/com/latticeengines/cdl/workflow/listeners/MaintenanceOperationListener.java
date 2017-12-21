package com.latticeengines.cdl.workflow.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("maintenanceOperationListener")
public class MaintenanceOperationListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceOperationListener.class);

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {

    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        String customerSpace = job.getTenant().getId();
        String status = job.getOutputContextValue(WorkflowContextConstants.Outputs.DATAFEED_STATUS);
        if (jobExecution.getStatus().isUnsuccessful()) {
            if (!StringUtils.isEmpty(status)) {
                //reset data feed status.
                dataFeedProxy.updateDataFeedStatus(customerSpace, status);
            }
        } else if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            //check data feed status? should be Initing or InitialLoaded?

        } else {
            log.error(String.format("CDL maintenance job ends in unknown status: %s",
                    jobExecution.getStatus().name()));
        }

        dataFeedProxy.updateDataFeedMaintenanceMode(customerSpace,false);
    }
}
