package com.latticeengines.cdl.workflow.steps;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("startExecution")
public class StartExecution extends BaseWorkflowStep<StartExecutionConfiguration> {

    @Autowired
    private DataFeedProxy datafeedProxy;

    @Override
    public void execute() {
        DataFeedExecution execution = datafeedProxy.startExecution(configuration.getCustomerSpace().toString(),
                configuration.getDataFeedName());
        if (execution == null) {
            putObjectInContext(CONSOLIDATE_INPUT_TABLES, Collections.EMPTY_LIST);
        } else {
            List<Table> importTables = execution.getImports().stream().map(i -> i.getImportData())
                    .collect(Collectors.toList());
            putObjectInContext(CONSOLIDATE_INPUT_TABLES, importTables);
        }
    }

}