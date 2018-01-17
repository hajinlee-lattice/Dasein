package com.latticeengines.cdl.workflow.steps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.CreateCdlEventTableFilterParameters;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CreateCdlEventTableFilterConfiguration;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("createCdlEventTableFilterStep")
public class CreateCdlEventTableFilterStep extends RunDataFlow<CreateCdlEventTableFilterConfiguration> {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(CreateCdlEventTableFilterStep.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private CreateCdlTableHelper createCdlTableHelper;

    private Table trainFilterTable;
    private Table eventFilterTable;

    @Override
    public void onConfigurationInitialized() {
        CreateCdlEventTableFilterConfiguration configuration = getConfiguration();
        configuration.setTargetTableName("CreateCdlEventTableFilter_" + System.currentTimeMillis());
        configuration.setDataFlowParams(createDataFlowParameters());
    }

    private DataFlowParameters createDataFlowParameters() {
        trainFilterTable = getTrainFilterTable();
        eventFilterTable = getEventFilterTable();
        CreateCdlEventTableFilterParameters parameters = new CreateCdlEventTableFilterParameters(
                trainFilterTable.getName(), eventFilterTable.getName());
        parameters.setEventColumn(configuration.getEventColumn());
        return parameters;
    }

    private Table getTrainFilterTable() {
        return createCdlTableHelper.getFilterTable(configuration.getCustomerSpace(), "RatingEngineModelTrainFilter",
                "_train_filter", configuration.getTrainFilterTableName(), configuration.getTrainQuery(),
                InterfaceName.Train, configuration.getTargetTableName(), false);
    }

    private Table getEventFilterTable() {
        return createCdlTableHelper.getFilterTable(configuration.getCustomerSpace(), "RatingEngineModelTargetFilter",
                "_event_filter", configuration.getEventFilterTableName(), configuration.getEventQuery(),
                InterfaceName.Event, configuration.getTargetTableName(), configuration.isExpectedValue());
    }

    @Override
    public void onExecutionCompleted() {
        Table filterTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getTargetTableName());
        putObjectInContext(FILTER_EVENT_TABLE, filterTable);
        metadataProxy.deleteTable(configuration.getCustomerSpace().toString(), trainFilterTable.getName());
        metadataProxy.deleteTable(configuration.getCustomerSpace().toString(), eventFilterTable.getName());
    }

}
