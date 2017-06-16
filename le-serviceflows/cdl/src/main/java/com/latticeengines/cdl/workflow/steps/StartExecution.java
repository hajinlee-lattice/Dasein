package com.latticeengines.cdl.workflow.steps;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeed.Status;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.DataFeedImport;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.StartExecutionConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.ExportDataToRedshiftConfiguration;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("startExecution")
public class StartExecution extends BaseWorkflowStep<StartExecutionConfiguration> {

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public void execute() {
        DataFeed datafeed = metadataProxy.findDataFeedByName(configuration.getCustomerSpace().toString(),
                configuration.getDataFeedName());
        boolean initialLoaded = Status.InitialLoaded.equals(datafeed.getStatus());
        putObjectInContext(DATA_INITIAL_LOADED, initialLoaded);

        ExportDataToRedshiftConfiguration exportDataToRedshiftConfig = getConfigurationFromJobParameters(
                ExportDataToRedshiftConfiguration.class);
        exportDataToRedshiftConfig.setSkipStep(initialLoaded);
        putObjectInContext(ExportDataToRedshiftConfiguration.class.getName(), exportDataToRedshiftConfig);

        DataFeedExecution execution = datafeed.getActiveExecution();
        if (execution == null) {
            putObjectInContext(CONSOLIDATE_INPUT_TABLES, Collections.EMPTY_LIST);
        } else {
            List<Table> importTables = execution.getImports().stream().map(DataFeedImport::getDataTable)
                    .collect(Collectors.toList());
            putObjectInContext(CONSOLIDATE_INPUT_TABLES, importTables);
        }
    }

}