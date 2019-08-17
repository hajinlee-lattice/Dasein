package com.latticeengines.cdl.workflow.listeners;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.pls.AdditionalEmailInfo;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.pls.PlsInternalProxy;
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
    private PlayProxy playProxy;

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private PlsInternalProxy plsInternalProxy;

    private String customerSpace;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        String initialDataFeedStatus = job.getInputContextValue(WorkflowContextConstants.Inputs.DATAFEED_STATUS);
        boolean isAlwaysOnCampaign =
                Boolean.parseBoolean(job.getInputContextValue(WorkflowContextConstants.Inputs.ALWAYS_ON_CAMPAIGNS));

        customerSpace = job.getTenant().getId();

        String tenantId = jobExecution.getJobParameters().getString("CustomerSpace");
        String userId = jobExecution.getJobParameters().getString("User_Id");
        log.info(String.format("tenantId %s, userId %s", tenantId, userId));

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
            if (execution.getStatus() != DataFeedExecution.Status.Completed) {
                throw new RuntimeException("Can't finish execution");
            }
            cleanupInactiveVersion();
            if (isAlwaysOnCampaign) {
                try {
                    playProxy.launchAlwaysOn(customerSpace);
                } catch (Exception e) {
                    log.error("Cannot update play launch channel for: " + customerSpace);
                }
            }
        } else {
            log.warn("Workflow ended in an unknown state.");
        }

        AdditionalEmailInfo emailInfo = new AdditionalEmailInfo();
        emailInfo.setUserId(userId);
        try {
            plsInternalProxy.sendCDLProcessAnalyzeEmail(jobExecution.getStatus().name(), tenantId, emailInfo);
        } catch (Exception e) {
            log.error("Can not send process analyze email: " + e.getMessage());
        }
    }

    private void cleanupInactiveVersion() {
        DataCollection.Version inactive = dataCollectionProxy.getInactiveVersion(customerSpace);
        for (TableRoleInCollection role : TableRoleInCollection.values()) {
            if (!isServingStore(role)) {
                List<String> tableNames = dataCollectionProxy.getTableNames(customerSpace, role, inactive);
                if (CollectionUtils.isNotEmpty(tableNames)) {
                    List<String> tablesInActive = dataCollectionProxy.getTableNames(customerSpace, role,
                            inactive.complement());
                    if (CollectionUtils.isNotEmpty(tablesInActive)) {
                        Set<String> activeTableSet = new HashSet<>(tablesInActive);
                        tableNames.forEach(tableName -> {
                            if (!activeTableSet.contains(tableName)) {
                                log.info("Removing table " + tableName + " as " + role + " in " + inactive);
                                metadataProxy.deleteTable(customerSpace, tableName);
                            } else {
                                log.info("Unlinking table " + tableName + " as " + role + " from " + inactive);
                                dataCollectionProxy.unlinkTable(customerSpace, tableName, role, inactive);
                            }
                        });
                    }
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
