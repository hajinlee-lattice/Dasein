package com.latticeengines.cdl.workflow.steps;

import java.util.Collections;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.service.ConvertBatchStoreService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.RegisterImportActionStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("registerImportActionStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RegisterImportActionStep extends BaseWorkflowStep<RegisterImportActionStepConfiguration> {

    @Inject
    private ActionProxy actionProxy;

    @SuppressWarnings("unchecked")
    @Override
    public void execute() {
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        ConvertBatchStoreService convertBatchStoreService =
                ConvertBatchStoreService.getConvertService(configuration.getConvertServiceConfig().getClass());
        if (convertBatchStoreService == null) {
            throw new RuntimeException("Cannot find ConvertBatchStore Service for config: " +
                    configuration.getConvertServiceConfig().getClass().getSimpleName());
        }
        Long actionPid = configuration.getActionPid();
        Action action =
                actionProxy.getActionsByPids(customerSpace.toString(), Collections.singletonList(actionPid)).get(0);
        if (action == null) {
            throw new RuntimeException("Cannot find action with PID: " + actionPid);
        }

        ImportActionConfiguration importConfig = new ImportActionConfiguration();

        importConfig.setWorkflowId(jobId);
        importConfig.setDataFeedTaskId(convertBatchStoreService.getOutputDataFeedTaskId(customerSpace.toString(),
                configuration.getConvertServiceConfig()));
        importConfig.setImportCount(convertBatchStoreService.getImportCounts(customerSpace.toString(),
                configuration.getConvertServiceConfig()));
        importConfig.setRegisteredTables(convertBatchStoreService.getRegisteredDataTables(customerSpace.toString(),
                configuration.getConvertServiceConfig()));

        action.setActionConfiguration(importConfig);
        actionProxy.updateAction(customerSpace.toString(), action);
    }
}
