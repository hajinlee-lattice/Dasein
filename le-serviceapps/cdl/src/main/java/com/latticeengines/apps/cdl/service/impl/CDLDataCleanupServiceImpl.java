package com.latticeengines.apps.cdl.service.impl;

import java.util.HashSet;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.CDLDataCleanupService;
import com.latticeengines.apps.cdl.workflow.CDLOperationWorkflowSubmitter;
import com.latticeengines.apps.cdl.workflow.RegisterDeleteDataWorkflowSubmitter;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupByDateRangeConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupByUploadConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupOperationConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.CleanupActionConfiguration;
import com.latticeengines.domain.exposed.pls.LegacyDeleteActionConfiguration;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;
import com.latticeengines.security.exposed.service.TenantService;

@Component("cdlDataCleanupService")
public class CDLDataCleanupServiceImpl implements CDLDataCleanupService {

    private static final Logger log = LoggerFactory.getLogger(CDLDataCleanupServiceImpl.class);

    @Inject
    private TenantService tenantService;

    @Inject
    private ActionService actionService;

    @Inject
    private SourceFileProxy sourceFileProxy;

    @Inject
    private RegisterDeleteDataWorkflowSubmitter registerDeleteDataWorkflowSubmitter;

    private final CDLOperationWorkflowSubmitter cdlOperationWorkflowSubmitter;

    @Inject
    public CDLDataCleanupServiceImpl(CDLOperationWorkflowSubmitter cdlOperationWorkflowSubmitter) {
        this.cdlOperationWorkflowSubmitter = cdlOperationWorkflowSubmitter;
    }

    @Override
    public ApplicationId cleanupData(String customerSpace, CleanupOperationConfiguration configuration) {
        log.info("customerSpace: " + customerSpace + ", CleanupOperationConfiguration: " + configuration);
        if (configuration instanceof CleanupByDateRangeConfiguration) {
            verifyCleanupByDataRangeConfiguration((CleanupByDateRangeConfiguration) configuration);
        }
        return cdlOperationWorkflowSubmitter.submit(CustomerSpace.parse(customerSpace), configuration,
                new WorkflowPidWrapper(-1L));
    }

    @Override
    public void createReplaceAction(String customerSpace, CleanupOperationConfiguration configuration) {
        //create delete action do not run workflow, waiting for PA to delete
        log.info("customerSpace: {}, CleanupOperationConfiguration: {}", customerSpace, configuration);
        log.info("Registering an operation action for tenant={}", customerSpace);
        Tenant tenant = tenantService.findByTenantId(customerSpace);
        if (tenant == null) {
            throw new NullPointerException(
                    String.format("Tenant with id=%s cannot be found", customerSpace));
        }
        BusinessEntity businessEntity = configuration.getEntity();
        if (businessEntity == null) {
            createReplaceAction(tenant, configuration.getOperationInitiator(), BusinessEntity.Account);
            createReplaceAction(tenant, configuration.getOperationInitiator(),
                    BusinessEntity.Contact);
            createReplaceAction(tenant, configuration.getOperationInitiator(), BusinessEntity.Product);
            createReplaceAction(tenant, configuration.getOperationInitiator(), BusinessEntity.Transaction);
        } else {
            createReplaceAction(tenant, configuration.getOperationInitiator(), businessEntity);
        }
    }

    @Override
    public ApplicationId createLegacyDeleteUploadAction(String customerSpace, CleanupOperationConfiguration configuration) {
        //delete lagacy tenant deleteByUpload Action, do not run workflow, waiting PA to do delete
        log.info("customerSpace: {}, CleanupOperationConfiguration: {}", customerSpace, configuration);
        log.info("Registering an operation action for tenant={}", customerSpace);
        Tenant tenant = tenantService.findByTenantId(customerSpace);
        if (tenant == null) {
            throw new NullPointerException(
                    String.format("Tenant with id=%s cannot be found", customerSpace));
        }
        Set<Long> actionPids = new HashSet<>();
        if (configuration instanceof CleanupByUploadConfiguration) {
            BusinessEntity businessEntity = configuration.getEntity();
            if (businessEntity == null) {
                actionPids.add(createLegacyDeleteUploadAction(tenant, (CleanupByUploadConfiguration) configuration,
                        BusinessEntity.Account));
                actionPids.add(createLegacyDeleteUploadAction(tenant, (CleanupByUploadConfiguration) configuration,
                        BusinessEntity.Contact));
                actionPids.add(createLegacyDeleteUploadAction(tenant, (CleanupByUploadConfiguration) configuration,
                        BusinessEntity.Product));
                actionPids.add(createLegacyDeleteUploadAction(tenant, (CleanupByUploadConfiguration) configuration,
                        BusinessEntity.Transaction));
            } else {
                actionPids.add(createLegacyDeleteUploadAction(tenant, (CleanupByUploadConfiguration) configuration,
                        businessEntity));
            }
        } else {
            throw new IllegalArgumentException(
                    String.format("Tenant with id=%s cannot find CleanupByUploadConfiguration when do delete " +
                            "operation.", customerSpace));
        }
        String sourceFileName = ((CleanupByUploadConfiguration) configuration).getFileName();
        SourceFile sourceFile = sourceFileProxy.findByName(customerSpace, sourceFileName);
        if (sourceFile == null) {
            log.error("Cannot find SourceFile with name: {}", sourceFileName);
            throw new RuntimeException("Cannot find SourceFile with name: " + sourceFileName);
        }
        if (StringUtils.isEmpty(sourceFile.getTableName())) {
            log.error("SourceFile: {} does not have a table object!", sourceFileName);
            throw new RuntimeException(String.format("SourceFile: %s does not have a table object!", sourceFileName));
        }
        return registerDeleteDataWorkflowSubmitter.legacyDeleteSubmit(CustomerSpace.parse(tenant.getId()), sourceFile,
                configuration.getOperationInitiator(), actionPids, new WorkflowPidWrapper((-1L)));
    }

    @Override
    public ApplicationId registerDeleteData(String customerSpace, boolean hardDelete, String sourceFileName, String user) {
        SourceFile sourceFile = sourceFileProxy.findByName(customerSpace, sourceFileName);
        if (sourceFile == null) {
            log.error("Cannot find SourceFile with name: {}", sourceFileName);
            throw new RuntimeException("Cannot find SourceFile with name: " + sourceFileName);
        }
        if (StringUtils.isEmpty(sourceFile.getTableName())) {
            log.error("SourceFile: {} does not have a table object!", sourceFileName);
            throw new RuntimeException(String.format("SourceFile: %s does not have a table object!", sourceFileName));
        }
        return registerDeleteDataWorkflowSubmitter.submit(CustomerSpace.parse(customerSpace), hardDelete,
                sourceFile, user, new WorkflowPidWrapper(-1L));
    }

    private void verifyCleanupByDataRangeConfiguration(
            CleanupByDateRangeConfiguration cleanupByDateRangeConfiguration) {
        if (cleanupByDateRangeConfiguration.getStartTime() == null
                || cleanupByDateRangeConfiguration.getEndTime() == null) {
            throw new LedpException(LedpCode.LEDP_40002);
        }

        if (cleanupByDateRangeConfiguration.getStartTime().getTime() > cleanupByDateRangeConfiguration.getEndTime()
                .getTime()) {
            throw new LedpException(LedpCode.LEDP_40003);
        }
    }

    private void createReplaceAction(Tenant tenant, String operationInitiator, BusinessEntity businessEntity) {
        Action action = new Action();
        action.setType(ActionType.DATA_REPLACE);
        action.setActionInitiator(operationInitiator);
        CleanupActionConfiguration cleanupActionConfiguration = new CleanupActionConfiguration();
        cleanupActionConfiguration.addImpactEntity(businessEntity);
        action.setActionConfiguration(cleanupActionConfiguration);
        action.setTenant(tenant);
        if (tenant.getPid() != null) {
            MultiTenantContext.setTenant(tenant);
        } else {
            log.warn("The tenant in action does not have a pid:{}. ", tenant);
        }
        actionService.create(action);
    }

    private Long createLegacyDeleteUploadAction(Tenant tenant,
                                                CleanupByUploadConfiguration cleanupByUploadConfiguration,
                                               BusinessEntity businessEntity) {
        Action action = new Action();
        action.setType(ActionType.LEGACY_DELETE_UPLOAD);
        action.setActionInitiator(cleanupByUploadConfiguration.getOperationInitiator());
        LegacyDeleteActionConfiguration legacyDeleteActionConfiguration = new LegacyDeleteActionConfiguration();
        legacyDeleteActionConfiguration.setEntity(businessEntity);
        legacyDeleteActionConfiguration.setTableName(cleanupByUploadConfiguration.getTableName());
        legacyDeleteActionConfiguration.setCleanupOperationType(cleanupByUploadConfiguration.getCleanupOperationType());
        legacyDeleteActionConfiguration.setFileDisplayName(cleanupByUploadConfiguration.getFileDisplayName());
        legacyDeleteActionConfiguration.setFileName(cleanupByUploadConfiguration.getFileName());
        legacyDeleteActionConfiguration.setFilePath(cleanupByUploadConfiguration.getFilePath());
        action.setActionConfiguration(legacyDeleteActionConfiguration);
        action.setTenant(tenant);
        if (tenant.getPid() != null) {
            MultiTenantContext.setTenant(tenant);
        } else {
            log.warn("The tenant in action does not have a pid:{}. ", tenant);
        }
        actionService.create(action);
        return action.getPid();
    }
}
