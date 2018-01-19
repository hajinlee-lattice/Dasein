package com.latticeengines.apps.cdl.workflow;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupByUploadConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupOperationConfiguration;
import com.latticeengines.domain.exposed.cdl.MaintenanceOperationConfiguration;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.CDLOperationWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.security.exposed.service.TenantService;

@Component
public class CDLOperationWorkflowSubmitter extends WorkflowSubmitter {
    private static final Logger log = LoggerFactory.getLogger(CDLOperationWorkflowSubmitter.class);

    @Inject
    private TenantService tenantService;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceProxy;

    @PostConstruct
    public void init() {
        internalResourceProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    public ApplicationId submit(CustomerSpace customerSpace,
            MaintenanceOperationConfiguration maintenanceOperationConfiguration) {
        log.info("generate Configuration");
        Action action = registerAction(customerSpace, maintenanceOperationConfiguration);
        log.info(String.format("Action=%s", action));
        CDLOperationWorkflowConfiguration configuration = generateConfiguration(customerSpace,
                maintenanceOperationConfiguration, action.getPid());

        log.info("submit Configuration");
        return workflowJobService.submit(configuration);
    }

    private Action registerAction(CustomerSpace customerSpace,
            MaintenanceOperationConfiguration maintenanceOperationConfiguration) {
        log.info(String.format("Registering an operation action for tenant=%s", customerSpace.toString()));
        Action action = new Action();
        action.setType(ActionType.CDL_OPERATION_WORKFLOW);
        action.setActionInitiator(maintenanceOperationConfiguration.getOperationInitiator());
        Tenant tenant = tenantService.findByTenantId(customerSpace.toString());
        if (tenant == null) {
            throw new NullPointerException(
                    String.format("Tenant with id=%s cannot be found", customerSpace.toString()));
        }
        action.setTenant(tenant);
        return internalResourceProxy.createAction(tenant.getId(), action);
    }

    private CDLOperationWorkflowConfiguration generateConfiguration(CustomerSpace customerSpace,
            MaintenanceOperationConfiguration maintenanceOperationConfiguration, @NotNull Long actionPid) {
        boolean isCleanupByUpload = false;
        if (maintenanceOperationConfiguration instanceof CleanupOperationConfiguration) {
            log.info("Configuratin is CleanupOperation");
            isCleanupByUpload = ((CleanupOperationConfiguration) maintenanceOperationConfiguration)
                    .getCleanupOperationType().isNeedTransFlow();
        }
        String filePath = "";
        String tableName = "";
        if (maintenanceOperationConfiguration instanceof CleanupByUploadConfiguration) {
            log.info("Configuratin is CleanupByUpload");
            CleanupByUploadConfiguration cleanupByUploadConfiguration = ((CleanupByUploadConfiguration) maintenanceOperationConfiguration);
            filePath = cleanupByUploadConfiguration.getFilePath();
            tableName = cleanupByUploadConfiguration.getTableName();
        }
        return new CDLOperationWorkflowConfiguration.Builder() //
                .customer(customerSpace) //
                .internalResourceHostPort(internalResourceHostPort) //
                .microServiceHostPort(microserviceHostPort) //
                .maintenanceOperationConfiguration(maintenanceOperationConfiguration) //
                .isCleanupByUpload(isCleanupByUpload) //
                .filePath(filePath) //
                .tableName(tableName) //
                .inputProperties(ImmutableMap.<String, String> builder() //
                        .put(WorkflowContextConstants.Inputs.ACTION_ID, actionPid.toString()) //
                        .build())
                .build();
    }
}
