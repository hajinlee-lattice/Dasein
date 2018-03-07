package com.latticeengines.cdl.workflow.steps.maintenance;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.StartMaintenanceConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("startMaintenanceStep")
public class StartMaintenanceStep extends BaseWorkflowStep<StartMaintenanceConfiguration> {

    @Autowired
    public DataFeedProxy dataFeedProxy;

    @Override
    public void execute() {
        String customerSpaceStr = configuration.getCustomerSpace().toString();
        DataFeed dataFeed = dataFeedProxy.getDataFeed(customerSpaceStr);
        if (dataFeed.isMaintenanceMode()) {
            throw new RuntimeException(String.format("Tenant %s already in maintenance mode", customerSpaceStr));
        } else {
            dataFeedProxy.updateDataFeedMaintenanceMode(customerSpaceStr, true);
        }

        saveOutputValue(WorkflowContextConstants.Outputs.IMPACTED_BUSINESS_ENTITIES,
                JsonUtils.serialize(configuration.getEntityList()));
    }

}
