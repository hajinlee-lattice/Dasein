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
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("startExecution")
public class StartExecution extends BaseWorkflowStep<StartExecutionConfiguration> {

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Override
    public void execute() {
        boolean isActive = Status.Active.equals(configuration.getInitialDataFeedStatus());
        putObjectInContext(IS_ACTIVE, isActive);
        ExportDataToRedshiftConfiguration exportDataToRedshiftConfig = getConfigurationFromJobParameters(
                ExportDataToRedshiftConfiguration.class);
        exportDataToRedshiftConfig.setSkipStep(!isActive);
        putObjectInContext(ExportDataToRedshiftConfiguration.class.getName(), exportDataToRedshiftConfig);

        DataFeedExecution execution = dataFeedProxy.updateExecutionWorkflowId(
                configuration.getCustomerSpace().toString(), jobId);
        log.info(String.format("current running execution %s", execution));

        DataFeed datafeed = dataFeedProxy.getDataFeed(configuration.getCustomerSpace().toString());
        execution = datafeed.getActiveExecution();

        if (execution == null) {
            putObjectInContext(CONSOLIDATE_INPUT_TABLES, Collections.EMPTY_LIST);
        } else if (execution.getWorkflowId().longValue() != jobId.longValue()) {
            throw new RuntimeException(
                    String.format("current active execution has a workflow id %s, which is different from %s ",
                            execution.getWorkflowId(), jobId));
        } else {
            List<Table> importTables = execution.getImports().stream().map(DataFeedImport::getDataTable)
                    .collect(Collectors.toList());
            putObjectInContext(CONSOLIDATE_INPUT_TABLES, importTables);
        }
    }

}