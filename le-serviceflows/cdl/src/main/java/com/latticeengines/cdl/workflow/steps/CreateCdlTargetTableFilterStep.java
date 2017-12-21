package com.latticeengines.cdl.workflow.steps;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CreateCdlTargetTableFilterConfiguration;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("createCdlTargetTableFilterStep")
public class CreateCdlTargetTableFilterStep extends BaseWorkflowStep<CreateCdlTargetTableFilterConfiguration> {

    @Autowired
    private CreateCdlTableHelper createCdlTableHelper;

    @Override
    public void onConfigurationInitialized() {
        configuration.setTargetTableName("CreateCdlTargetTableFilter_" + System.currentTimeMillis());
    }

    @Override
    public void execute() {
        Table targetFilterTable = getTargetFilterTable();
        putObjectInContext(FILTER_EVENT_TABLE, targetFilterTable);
    }

    private Table getTargetFilterTable() {
        return createCdlTableHelper.getFilterTable(configuration.getCustomerSpace(), "RatingEngineModelTargetFilter",
                "_target_filter", configuration.getTargetFilterTableName(), configuration.getTargetQuery(),
                InterfaceName.Target, configuration.getTargetTableName());
    }

}
