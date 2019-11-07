package com.latticeengines.apps.cdl.workflow;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.DeleteActionConfiguration;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.RegisterDeleteDataWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.security.exposed.service.TenantService;

@Component
public class RegisterDeleteDataWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(RegisterDeleteDataWorkflowSubmitter.class);

    @Inject
    private ActionService actionService;

    @Inject
    private TenantService tenantService;

    @WithWorkflowJobPid
    public ApplicationId submit(CustomerSpace customerSpace, boolean hardDelete,
                                SourceFile sourceFile, String user, WorkflowPidWrapper pidWrapper) {
        Action action = registerAction(customerSpace, sourceFile.getTableName(), hardDelete, user, pidWrapper.getPid());
        RegisterDeleteDataWorkflowConfiguration configuration = generateConfig(customerSpace,
                sourceFile.getTableName(), sourceFile.getPath(), user, action.getPid());
        return workflowJobService.submit(configuration, pidWrapper.getPid());
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
}
