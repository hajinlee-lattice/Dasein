package com.latticeengines.cdl.workflow.steps;

import java.util.Collections;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.MigrateTracking;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.RegisterImportActionStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.MigrateTrackingProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("registerImportActionStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RegisterImportActionStep extends BaseWorkflowStep<RegisterImportActionStepConfiguration> {

    @Inject
    private ActionProxy actionProxy;

    @Inject
    private MigrateTrackingProxy migrateTrackingProxy;

    @Override
    public void execute() {
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        Long actionPid = configuration.getActionPid();
        Action action =
                actionProxy.getActionsByPids(customerSpace.toString(), Collections.singletonList(actionPid)).get(0);
        if (action == null) {
            throw new RuntimeException("Cannot find action with PID: " + actionPid);
        }
        MigrateTracking migrateTracking = migrateTrackingProxy.getMigrateTracking(customerSpace.toString(),
                configuration.getMigrateTrackingPid());
        if (migrateTracking == null || migrateTracking.getReport() == null) {
            throw new RuntimeException("Migrate Tracking Record is not correctly created!");
        }

        ImportActionConfiguration importConfig = new ImportActionConfiguration();

        importConfig.setWorkflowId(jobId);
        switch (configuration.getEntity()) {
            case Account:
                importConfig.setDataFeedTaskId(migrateTracking.getReport().getOutputAccountTaskId());
                importConfig.setImportCount(migrateTracking.getReport().getAccountCounts());
                importConfig.setRegisteredTables(migrateTracking.getReport().getAccountDataTables());
                break;
            case Contact:
                importConfig.setDataFeedTaskId(migrateTracking.getReport().getOutputContactTaskId());
                importConfig.setImportCount(migrateTracking.getReport().getContactCounts());
                importConfig.setRegisteredTables(migrateTracking.getReport().getContactDataTables());
                break;
            case Transaction:
                importConfig.setDataFeedTaskId(migrateTracking.getReport().getOutputTransactionTaskId());
                importConfig.setImportCount(migrateTracking.getReport().getTransactionCounts());
                importConfig.setRegisteredTables(migrateTracking.getReport().getTransactionDataTables());
                break;
            default:
                throw new IllegalArgumentException("Not supported Entity for migrate!");

        }

        action.setActionConfiguration(importConfig);
        actionProxy.updateAction(customerSpace.toString(), action);
    }
}
