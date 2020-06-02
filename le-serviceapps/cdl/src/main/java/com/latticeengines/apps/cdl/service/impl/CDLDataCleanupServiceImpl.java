package com.latticeengines.apps.cdl.service.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.cdl.service.CDLDataCleanupService;
import com.latticeengines.apps.cdl.workflow.CDLOperationWorkflowSubmitter;
import com.latticeengines.apps.cdl.workflow.RegisterDeleteDataWorkflowSubmitter;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupByDateRangeConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupByUploadConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupOperationConfiguration;
import com.latticeengines.domain.exposed.cdl.DeleteRequest;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.FakeApplicationId;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.CleanupActionConfiguration;
import com.latticeengines.domain.exposed.pls.DeleteActionConfiguration;
import com.latticeengines.domain.exposed.pls.LegacyDeleteByDateRangeActionConfiguration;
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

    @Inject
    private BatonService batonService;

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
        if (isEntityMatchEnabled(customerSpace)) {
            throw new IllegalStateException("entityMatch tenant cannot create legacyDelete Action.");
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
        return registerDeleteDataWorkflowSubmitter.legacyDeleteSubmit(CustomerSpace.parse(customerSpace), sourceFile,
                configuration.getOperationInitiator(), configuration, new WorkflowPidWrapper(-1L));
    }

    @Override
    public void createLegacyDeleteDateRangeAction(String customerSpace, CleanupOperationConfiguration configuration) {
        if (isEntityMatchEnabled(customerSpace)) {
            throw new IllegalStateException("entityMatch tenant cannot create legacyDelete Action.");
        }
        log.info("customerSpace: {}, CleanupOperationConfiguration: {}", customerSpace, configuration);
        log.info("Registering an operation action for tenant={}", customerSpace);
        Tenant tenant = tenantService.findByTenantId(customerSpace);
        if (tenant == null) {
            throw new NullPointerException(
                    String.format("Tenant with id=%s cannot be found", customerSpace));
        }
        if (configuration instanceof CleanupByDateRangeConfiguration) {
            BusinessEntity businessEntity = configuration.getEntity();
            if (businessEntity == null) {
                createLegacyDeleteByDateRangeAction(tenant, (CleanupByDateRangeConfiguration) configuration,
                        BusinessEntity.Account);
                createLegacyDeleteByDateRangeAction(tenant, (CleanupByDateRangeConfiguration) configuration,
                        BusinessEntity.Contact);
                createLegacyDeleteByDateRangeAction(tenant, (CleanupByDateRangeConfiguration) configuration,
                        BusinessEntity.Product);
                createLegacyDeleteByDateRangeAction(tenant, (CleanupByDateRangeConfiguration) configuration,
                        BusinessEntity.Transaction);
            } else {
                createLegacyDeleteByDateRangeAction(tenant, (CleanupByDateRangeConfiguration) configuration,
                        businessEntity);
            }
        } else {
            throw new IllegalArgumentException(
                    String.format("Tenant with id=%s cannot find CleanupByDataRangeConfiguration when do delete " +
                            "operation.", customerSpace));
        }
    }

    @Override
    public ApplicationId registerDeleteData(String customerSpace, DeleteRequest request) {
        String sourceFileName = request.getFilename();
        if (StringUtils.isBlank(sourceFileName)) {
            // TODO check time range exists
            Action action = createTimeRangeDeleteAction(CustomerSpace.parse(customerSpace), request);
            log.info("Delete by time range action created successfully = {}", JsonUtils.serialize(action));
            // TODO maybe figure out a better fake ID
            return new FakeApplicationId(-1L);
        }
        SourceFile sourceFile = sourceFileProxy.findByName(customerSpace, sourceFileName);
        if (sourceFile == null) {
            log.error("Cannot find SourceFile with name: {}", sourceFileName);
            throw new RuntimeException("Cannot find SourceFile with name: " + sourceFileName);
        }
        if (StringUtils.isEmpty(sourceFile.getTableName())) {
            log.error("SourceFile: {} does not have a table object!", sourceFileName);
            throw new RuntimeException(String.format("SourceFile: %s does not have a table object!", sourceFileName));
        }
        request.setSourceFile(sourceFile);
        return registerDeleteDataWorkflowSubmitter.submit(CustomerSpace.parse(customerSpace), request, new WorkflowPidWrapper(-1L));
    }

    private Action createTimeRangeDeleteAction(@NotNull CustomerSpace customerSpace, @NotNull DeleteRequest request) {
        Action action = new Action();
        action.setType(ActionType.SOFT_DELETE);
        action.setActionInitiator(request.getUser());
        Tenant tenant = tenantService.findByTenantId(customerSpace.toString());
        MultiTenantContext.setTenant(tenant);
        Preconditions.checkNotNull(tenant,
                String.format("Tenant with id=%s cannot be found", customerSpace.toString()));
        DeleteActionConfiguration deleteConfig = new DeleteActionConfiguration();
        deleteConfig.setDeleteEntities(request.getDeleteEntities());
        deleteConfig.setDeleteStreamIds(request.getDeleteStreamIds());
        deleteConfig.setDeleteEntityType(request.getDeleteEntityType());
        deleteConfig.setFromDate(request.getFromDate());
        deleteConfig.setToDate(request.getToDate());
        action.setActionConfiguration(deleteConfig);
        action.setTenant(tenant);
        return actionService.create(action);
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

    private void createLegacyDeleteByDateRangeAction(Tenant tenant,
                                                     CleanupByDateRangeConfiguration cleanupByDateRangeConfiguration,
                                                     BusinessEntity entity) {
        Action action = new Action();
        action.setType(ActionType.LEGACY_DELETE_DATERANGE);
        action.setActionInitiator(cleanupByDateRangeConfiguration.getOperationInitiator());
        LegacyDeleteByDateRangeActionConfiguration legacyDeleteByDateRangeActionConfiguration =
                new LegacyDeleteByDateRangeActionConfiguration();
        legacyDeleteByDateRangeActionConfiguration.setEntity(entity);
        legacyDeleteByDateRangeActionConfiguration.setStartTime(cleanupByDateRangeConfiguration.getStartTime());
        legacyDeleteByDateRangeActionConfiguration.setEndTime(cleanupByDateRangeConfiguration.getEndTime());
        action.setActionConfiguration(legacyDeleteByDateRangeActionConfiguration);
        action.setTenant(tenant);
        if (tenant.getPid() != null) {
            MultiTenantContext.setTenant(tenant);
        } else {
            log.warn("The tenant in action does not have a pid:{}. ", tenant);
        }
        actionService.create(action);
    }

    private boolean isEntityMatchEnabled(String customerSpace) {
        return batonService.isEntityMatchEnabled(CustomerSpace.parse(customerSpace)) && !batonService.onlyEntityMatchGAEnabled(CustomerSpace.parse(customerSpace));
    }

}
