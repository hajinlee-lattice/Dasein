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
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
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

    @Inject
    private MetadataProxy metadataProxy;

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
            DataFeedExecution execution = dataFeedProxy.failExecution(customerSpace, initialDataFeedStatus);
            if (execution.getStatus() != DataFeedExecution.Status.Failed) {
                throw new RuntimeException("Can't fail execution");
            }
        } else if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info(String.format("Workflow completed. Update datafeed status for customer %s with status of %s",
                    customerSpace, Status.Active.getName()));
            DataFeedExecution execution = dataFeedProxy.finishExecution(customerSpace, initialDataFeedStatus);
            log.info(String.format("trying to finish running execution %s", execution));
            if (execution.getStatus() != DataFeedExecution.Status.ProcessAnalyzed) {
                throw new RuntimeException("Can't finish execution");
            }
            cleanupInactiveVersion();
        } else {
            log.warn("Workflow ended in an unknown state.");
        }
    }

    private void cleanupInactiveVersion() {
        DataCollection.Version inactive = dataCollectionProxy.getInactiveVersion(customerSpace);
        for (TableRoleInCollection role : TableRoleInCollection.values()) {
            if (!isServingStore(role)) {
                String tableName = dataCollectionProxy.getTableName(customerSpace, role, inactive);
                if (StringUtils.isNotBlank(tableName)) {
                    log.info("Removing table " + tableName + " as " + role + " in " + inactive);
                    metadataProxy.deleteTable(customerSpace, tableName);
                }
            }
        }
    }

    private boolean isServingStore(TableRoleInCollection role) {
        if (TableRoleInCollection.AnalyticPurchaseState.equals(role)) {
            return true;
        }
        for (BusinessEntity entity : BusinessEntity.values()) {
            if (role.equals(entity.getServingStore())) {
                return true;
            }
        }
        return false;
    }

}
