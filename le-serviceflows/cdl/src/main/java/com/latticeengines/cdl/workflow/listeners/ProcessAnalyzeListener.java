package com.latticeengines.cdl.workflow.listeners;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("processAnalyzeListener")
public class ProcessAnalyzeListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(ProcessAnalyzeListener.class);

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    private String customerSpace;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        String initialDataFeedStatus = job.getInputContextValue(WorkflowContextConstants.Inputs.DATAFEED_STATUS);
        customerSpace = job.getTenant().getId();

        if (jobExecution.getStatus() == BatchStatus.FAILED) {
            log.info(String.format("Workflow failed. Update datafeed status for customer %s with status of %s",
                    customerSpace, initialDataFeedStatus));
            dataFeedProxy.updateDataFeedStatus(customerSpace, initialDataFeedStatus);
        } else if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info(String.format("Workflow completed. Update datafeed status for customer %s with status of %s",
                    customerSpace, Status.Active.getName()));
            dataFeedProxy.updateDataFeedStatus(customerSpace, Status.Active.getName());
        } else {
            log.warn("Workflow ended in an unknown state.");
        }
    }

    private void swapMissingTableRoles() {
        DataCollection.Version active = dataCollectionProxy.getActiveVersion(customerSpace);
        DataCollection.Version inactive = active.complement();
        for (TableRoleInCollection role: TableRoleInCollection.values()) {
            String inactiveTableName = dataCollectionProxy.getTableName(customerSpace, role, inactive);
            if (StringUtils.isBlank(inactiveTableName)) {
                String activeTableName = dataCollectionProxy.getTableName(customerSpace, role, active);
                if (StringUtils.isNotBlank(activeTableName)) {
                    log.info("Swapping table " + activeTableName + " from " + active + " to " + inactive + " as " + role);
                    dataCollectionProxy.unlinkTable(customerSpace, activeTableName, role, active);
                    dataCollectionProxy.upsertTable(customerSpace, activeTableName, role, inactive);
                }
            }
        }
    }

}
