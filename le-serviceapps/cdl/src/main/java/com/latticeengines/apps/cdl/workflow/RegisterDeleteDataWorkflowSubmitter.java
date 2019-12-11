package com.latticeengines.apps.cdl.workflow;

import java.util.HashSet;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupByUploadConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupOperationConfiguration;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.DeleteActionConfiguration;
import com.latticeengines.domain.exposed.pls.LegacyDeleteActionConfiguration;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.RegisterDeleteDataWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.service.TenantService;

@Component
public class RegisterDeleteDataWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(RegisterDeleteDataWorkflowSubmitter.class);

    @Inject
    private ActionService actionService;

    @Inject
    private TenantService tenantService;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private SourceFileProxy sourceFileProxy;

    @WithWorkflowJobPid
    public ApplicationId submit(CustomerSpace customerSpace, boolean hardDelete,
                                SourceFile sourceFile, String user, WorkflowPidWrapper pidWrapper) {
        if (!checkSourceFile(customerSpace, sourceFile)) {
            throw new RuntimeException("Cannot Register delete data due to Source File issue!");
        }
        Action action = registerAction(customerSpace, sourceFile.getTableName(), hardDelete, user, pidWrapper.getPid());
        RegisterDeleteDataWorkflowConfiguration configuration = generateConfig(customerSpace,
                sourceFile.getTableName(), sourceFile.getPath(), user, action.getPid());
        return workflowJobService.submit(configuration, pidWrapper.getPid());
    }

    @WithWorkflowJobPid
    public ApplicationId legacyDeleteSubmit(CustomerSpace customerSpace, SourceFile sourceFile, String user,
                                            CleanupOperationConfiguration cleanupOperationConfiguration,
                                            WorkflowPidWrapper pidWrapper) {
        if (!checkSourceFile(customerSpace, sourceFile)) {
            throw new RuntimeException("Cannot Register delete data due to Source File issue!");
        }
        Set<Long> actionPids = registerLegacyDeleteAction(customerSpace.toString(), cleanupOperationConfiguration,
                pidWrapper.getPid());
        RegisterDeleteDataWorkflowConfiguration configuration = generateConfig(customerSpace,
                sourceFile.getTableName(), sourceFile.getPath(), user, actionPids);
        return workflowJobService.submit(configuration, pidWrapper.getPid());
    }

    private boolean checkSourceFile(CustomerSpace customerSpace, SourceFile sourceFile) {
        if (StringUtils.isEmpty(sourceFile.getTableName())) {
            log.error("There's no table attached to the source file {}!", sourceFile.getName());
            return false;
        }
        Table fileTable = metadataProxy.getTable(customerSpace.toString(), sourceFile.getTableName());
        if (fileTable == null) {
            log.error("The table attached to source file {} is empty!", sourceFile.getName());
            return false;
        }
        if (CollectionUtils.isEmpty(fileTable.getExtracts())) {
            return true;
        }
        Table fileTableWithoutExtract = metadataProxy.cloneTable(customerSpace.toString(), fileTable.getName(), true);
        sourceFile.setTableName(fileTableWithoutExtract.getName());
        sourceFileProxy.update(customerSpace.toString(), sourceFile);
        return true;
    }

    private Action registerAction(CustomerSpace customerSpace, String tableName, boolean hardDelete, String user,
                                  Long workflowPid) {
        Action action = new Action();
        if (hardDelete) {
            action.setType(ActionType.HARD_DELETE);
        } else {
            action.setType(ActionType.SOFT_DELETE);
        }
        action.setTrackingPid(workflowPid);
        action.setActionInitiator(user);
        Tenant tenant = tenantService.findByTenantId(customerSpace.toString());
        if (tenant == null) {
            throw new NullPointerException(
                    String.format("Tenant with id=%s cannot be found", customerSpace.toString()));
        }
        DeleteActionConfiguration deleteActionConfiguration = new DeleteActionConfiguration();
        deleteActionConfiguration.setDeleteDataTable(tableName);
        action.setActionConfiguration(deleteActionConfiguration);
        action.setTenant(tenant);
        if (tenant.getPid() != null) {
            MultiTenantContext.setTenant(tenant);
        } else {
            log.warn("The tenant in action does not have a pid: " + tenant);
        }
        return actionService.create(action);
    }

    private RegisterDeleteDataWorkflowConfiguration generateConfig(CustomerSpace customerSpace, String tableName,
                                                                   String filePath, String user, Long actionPid) {
        return new RegisterDeleteDataWorkflowConfiguration.Builder()
                .customer(customerSpace)
                .internalResourceHostPort(internalResourceHostPort) //
                .microServiceHostPort(microserviceHostPort) //
                .tableName(tableName)
                .filePath(filePath)
                .userId(user)
                .inputProperties(ImmutableMap.<String, String>builder() //
                        .put(WorkflowContextConstants.Inputs.ACTION_ID, actionPid.toString())
                        .build())//
                .build();
    }

    private RegisterDeleteDataWorkflowConfiguration generateConfig(CustomerSpace customerSpace, String tableName,
                                                                   String filePath, String user, Set<Long> actionPids) {
        return new RegisterDeleteDataWorkflowConfiguration.Builder()
                .customer(customerSpace)
                .internalResourceHostPort(internalResourceHostPort) //
                .microServiceHostPort(microserviceHostPort) //
                .tableName(tableName)
                .filePath(filePath)
                .userId(user)
                .inputProperties(ImmutableMap.<String, String>builder() //
                        .put(WorkflowContextConstants.Inputs.ACTION_IDS, JsonUtils.serialize(actionPids))
                        .build())//
                .build();
    }

    private Long createLegacyDeleteUploadAction(Tenant tenant,
                                                CleanupByUploadConfiguration cleanupByUploadConfiguration,
                                                BusinessEntity businessEntity, Long workflowPid) {
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
        action.setTrackingPid(workflowPid);
        action.setTenant(tenant);
        if (tenant.getPid() != null) {
            MultiTenantContext.setTenant(tenant);
        } else {
            log.warn("The tenant in action does not have a pid:{}. ", tenant);
        }
        actionService.create(action);
        return action.getPid();
    }

    private Set<Long> registerLegacyDeleteAction(String customerSpace, CleanupOperationConfiguration configuration,
                                                 Long workflowPid) {
        //delete lagacy tenant deleteByUpload Action, do not run workflow, waiting PA to do delete
        log.info("customerSpace: {}, CleanupOperationConfiguration: {}", customerSpace, configuration);
        log.info("Registering an operation action for tenant={}", customerSpace);
        Tenant tenant = tenantService.findByTenantId(customerSpace);
        Set<Long> actionPids = new HashSet<>();
        if (tenant == null) {
            throw new NullPointerException(
                    String.format("Tenant with id=%s cannot be found", customerSpace));
        }
        if (configuration instanceof CleanupByUploadConfiguration) {
            BusinessEntity businessEntity = configuration.getEntity();
            if (businessEntity == null) {
                actionPids.add(createLegacyDeleteUploadAction(tenant, (CleanupByUploadConfiguration) configuration,
                        BusinessEntity.Account, workflowPid));
                actionPids.add(createLegacyDeleteUploadAction(tenant, (CleanupByUploadConfiguration) configuration,
                        BusinessEntity.Contact, workflowPid));
                actionPids.add(createLegacyDeleteUploadAction(tenant, (CleanupByUploadConfiguration) configuration,
                        BusinessEntity.Product, workflowPid));
                actionPids.add(createLegacyDeleteUploadAction(tenant, (CleanupByUploadConfiguration) configuration,
                        BusinessEntity.Transaction, workflowPid));
            } else {
                actionPids.add(createLegacyDeleteUploadAction(tenant, (CleanupByUploadConfiguration) configuration,
                        businessEntity, workflowPid));
            }
        } else {
            throw new IllegalArgumentException(
                    String.format("Tenant with id=%s cannot find CleanupByUploadConfiguration when do delete " +
                            "operation.", customerSpace));
        }
        return actionPids;
    }
}
