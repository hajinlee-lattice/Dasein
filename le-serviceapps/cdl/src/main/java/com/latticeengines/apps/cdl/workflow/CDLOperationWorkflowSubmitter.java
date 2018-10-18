package com.latticeengines.apps.cdl.workflow;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupAllConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupByDateRangeConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupByUploadConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupOperationConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.cdl.MaintenanceOperationConfiguration;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.CleanupActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.CDLOperationWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.security.exposed.service.TenantService;

@Component
public class CDLOperationWorkflowSubmitter extends WorkflowSubmitter {
    private static final Logger log = LoggerFactory.getLogger(CDLOperationWorkflowSubmitter.class);

    @Inject
    private TenantService tenantService;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private ActionService actionService;

    @WithWorkflowJobPid
    public ApplicationId submit(CustomerSpace customerSpace,
            MaintenanceOperationConfiguration maintenanceOperationConfiguration, WorkflowPidWrapper pidWrapper) {
        if (customerSpace == null) {
            throw new IllegalArgumentException("The CustomerSpace cannot be null!");
        }
        log.info(String.format("CDLOperation WorkflowJob created for customer=%s with pid=%s", customerSpace,
                pidWrapper.getPid()));
        DataFeed dataFeed = dataFeedProxy.getDataFeed(customerSpace.toString());
        DataFeed.Status dataFeedStatus = dataFeed.getStatus();
        log.info(String.format("Current data feed: %s, status: %s", dataFeed.getName(), dataFeedStatus.getName()));
        if (!dataFeedProxy.lockExecution(customerSpace.toString(), DataFeedExecutionJobType.CDLOperation)) {
            String errorMessage;
            if (DataFeed.Status.ProcessAnalyzing.equals(dataFeedStatus)) {
                errorMessage = "You cannot perform delete action while PA is running";
            } else if (DataFeed.Status.Deleting.equals(dataFeedStatus)) {
                errorMessage = "You cannot perform delete action while another delete action is running";
            } else {
                errorMessage = String.format("We cannot start cleanup workflow for %s by dataFeedStatus %s",
                        customerSpace.toString(), dataFeedStatus.getName());
            }
            throw new RuntimeException(errorMessage);
        }

        DataFeed.Status initialStatus = getInitialDataFeedStatus(dataFeedStatus);
        log.info(String.format("data feed %s initial status: %s", dataFeed.getName(), initialStatus.getName()));

        Action action = registerAction(customerSpace, maintenanceOperationConfiguration, pidWrapper.getPid());
        log.info(String.format("Action=%s", action));
        CDLOperationWorkflowConfiguration configuration = generateConfiguration(customerSpace,
                maintenanceOperationConfiguration, action.getPid(), initialStatus);

        log.info(String.format("Submitting CDL operation workflow for customer %s", customerSpace));
        return workflowJobService.submit(configuration, pidWrapper.getPid());
    }

    private DataFeed.Status getInitialDataFeedStatus(DataFeed.Status status) {
        if (status.equals(DataFeed.Status.ProcessAnalyzing)) {
            return DataFeed.Status.Active;
        } else {
            return status;
        }
    }

    private Action registerAction(CustomerSpace customerSpace,
            MaintenanceOperationConfiguration maintenanceOperationConfiguration, Long workflowPid) {
        log.info(String.format("Registering an operation action for tenant=%s", customerSpace.toString()));
        Action action = new Action();
        action.setType(ActionType.CDL_OPERATION_WORKFLOW);
        action.setTrackingPid(workflowPid);
        action.setActionInitiator(maintenanceOperationConfiguration.getOperationInitiator());
        if (maintenanceOperationConfiguration instanceof CleanupOperationConfiguration) {
            CleanupActionConfiguration cleanupActionConfiguration = new CleanupActionConfiguration();
            BusinessEntity businessEntity = ((CleanupOperationConfiguration) maintenanceOperationConfiguration)
                    .getEntity();
            if (businessEntity == null) {
                cleanupActionConfiguration.addImpactEntity(BusinessEntity.Account);
                cleanupActionConfiguration.addImpactEntity(BusinessEntity.Contact);
                cleanupActionConfiguration.addImpactEntity(BusinessEntity.Product);
                cleanupActionConfiguration.addImpactEntity(BusinessEntity.Transaction);
            } else {
                cleanupActionConfiguration.addImpactEntity(businessEntity);
            }
            action.setActionConfiguration(cleanupActionConfiguration);
        }

        Tenant tenant = tenantService.findByTenantId(customerSpace.toString());
        if (tenant == null) {
            throw new NullPointerException(
                    String.format("Tenant with id=%s cannot be found", customerSpace.toString()));
        }
        action.setTenant(tenant);
        MultiTenantContext.setTenant(tenant);
        return actionService.create(action);
    }

    private CDLOperationWorkflowConfiguration generateConfiguration(CustomerSpace customerSpace,
            MaintenanceOperationConfiguration maintenanceOperationConfiguration, @NonNull Long actionPid,
            DataFeed.Status status) {
        boolean isCleanupByUpload = false;
        BusinessEntity businessEntity = null;
        if (maintenanceOperationConfiguration instanceof CleanupOperationConfiguration) {
            log.info("Configuratin is CleanupOperation");
            isCleanupByUpload = ((CleanupOperationConfiguration) maintenanceOperationConfiguration)
                    .getCleanupOperationType().isNeedTransFlow();
            businessEntity = ((CleanupOperationConfiguration) maintenanceOperationConfiguration).getEntity();
        }
        String filePath = "";
        String tableName = "";
        String fileName = generateFakeFileName(maintenanceOperationConfiguration);
        String fileDisplayName = generateFakeFileName(maintenanceOperationConfiguration);
        boolean isUseDLData = false;
        if (maintenanceOperationConfiguration instanceof CleanupByUploadConfiguration) {
            log.info("Configuratin is CleanupByUpload");
            CleanupByUploadConfiguration cleanupByUploadConfiguration = ((CleanupByUploadConfiguration) maintenanceOperationConfiguration);
            filePath = cleanupByUploadConfiguration.getFilePath();
            tableName = cleanupByUploadConfiguration.getTableName();
            fileName = cleanupByUploadConfiguration.getFileName();
            fileDisplayName = cleanupByUploadConfiguration.getFileDisplayName();
            isUseDLData = cleanupByUploadConfiguration.isUseDLData();
        }
        return new CDLOperationWorkflowConfiguration.Builder() //
                .customer(customerSpace) //
                .internalResourceHostPort(internalResourceHostPort) //
                .microServiceHostPort(microserviceHostPort) //
                .maintenanceOperationConfiguration(maintenanceOperationConfiguration) //
                .isCleanupByUpload(isCleanupByUpload, isUseDLData) //
                .filePath(filePath) //
                .tableName(tableName) //
                .businessEntity(businessEntity)
                .inputProperties(ImmutableMap.<String, String> builder() //
                        .put(WorkflowContextConstants.Inputs.ACTION_ID, actionPid.toString()) //
                        .put(WorkflowContextConstants.Inputs.SOURCE_FILE_NAME, fileName) //
                        .put(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME, fileDisplayName) //
                        .put(WorkflowContextConstants.Inputs.DATAFEED_STATUS, status.getName()).build())
                .build();
    }

    private String generateFakeFileName(MaintenanceOperationConfiguration maintenanceOperationConfiguration) {
        String fileName = "";
        if (maintenanceOperationConfiguration instanceof CleanupAllConfiguration) {
            CleanupAllConfiguration cleanupAllConfiguration = ((CleanupAllConfiguration) maintenanceOperationConfiguration);
            if (cleanupAllConfiguration.getEntity() == null) {
                if (cleanupAllConfiguration.getCleanupOperationType() == CleanupOperationType.ALL) {
                    fileName = "All Types, Full";
                } else if (cleanupAllConfiguration.getCleanupOperationType() == CleanupOperationType.ALLDATA) {
                    fileName = "All Types, Data Only";
                }
            } else {
                if (cleanupAllConfiguration.getCleanupOperationType() == CleanupOperationType.ALL) {
                    fileName = String.format("All %s, Full", cleanupAllConfiguration.getEntity().name());
                } else if (cleanupAllConfiguration.getCleanupOperationType() == CleanupOperationType.ALLDATA) {
                    fileName = String.format("All %s, Data Only", cleanupAllConfiguration.getEntity().name());
                }
            }
        } else if (maintenanceOperationConfiguration instanceof CleanupByDateRangeConfiguration) {
            CleanupByDateRangeConfiguration cleanupByDateRangeConfiguration = ((CleanupByDateRangeConfiguration) maintenanceOperationConfiguration);
            DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
            String start = dateFormat.format(cleanupByDateRangeConfiguration.getStartTime());
            String end = dateFormat.format(cleanupByDateRangeConfiguration.getEndTime());
            fileName = String.format("Transactions, Data during %s - %s", start, end);
        }
        return fileName;
    }
}
