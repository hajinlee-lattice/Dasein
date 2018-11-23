package com.latticeengines.cdl.workflow.steps.maintenance;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.StartMaintenanceConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("startMaintenanceStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class StartMaintenanceStep extends BaseWorkflowStep<StartMaintenanceConfiguration> {

    @Autowired
    public DataFeedProxy dataFeedProxy;

    @Autowired
    public DataCollectionProxy dataCollectionProxy;

    private CustomerSpace customerSpace;

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace();
        DataFeed dataFeed = dataFeedProxy.getDataFeed(customerSpace.toString());
        if (dataFeed.isMaintenanceMode()) {
            //todo: Remove the maintenance Mode flag. Already has a status lock in DataFeed.
            log.warn(String.format("Tenant %s already in maintenance mode", customerSpace.toString()));
        } else {
            dataFeedProxy.updateDataFeedMaintenanceMode(customerSpace.toString(), true);
        }
        dataFeedProxy.startExecution(customerSpace.toString(), DataFeedExecutionJobType.CDLOperation, jobId);
        setStepContext();
        saveOutputValue(WorkflowContextConstants.Outputs.IMPACTED_BUSINESS_ENTITIES,
                JsonUtils.serialize(configuration.getEntityList()));
    }

    private void setStepContext() {
        DataCollection.Version activeVersion = dataCollectionProxy.getActiveVersion(customerSpace.toString());
        DataCollection.Version inactiveVersion = activeVersion.complement();
        putLongValueInContext(CLEANUP_TIMESTAMP, System.currentTimeMillis());
        putObjectInContext(CDL_ACTIVE_VERSION, activeVersion);
        putObjectInContext(CDL_INACTIVE_VERSION, inactiveVersion);
        putObjectInContext(CUSTOMER_SPACE, customerSpace.toString());
        putObjectInContext(IMPACTED_ENTITIES, configuration.getEntityList());
    }

}
