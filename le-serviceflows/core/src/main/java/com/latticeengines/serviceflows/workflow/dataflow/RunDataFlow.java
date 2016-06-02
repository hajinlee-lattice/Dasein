package com.latticeengines.serviceflows.workflow.dataflow;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataflow.DataFlowConfiguration;
import com.latticeengines.domain.exposed.dataflow.DataFlowSource;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.proxy.exposed.dataflowapi.DataFlowApiProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

public class RunDataFlow<T extends DataFlowStepConfiguration> extends BaseWorkflowStep<T> {

    private static final Log log = LogFactory.getLog(RunDataFlow.class);

    @Autowired
    private DataFlowApiProxy dataFlowApiProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public void execute() {
        log.info("Inside RunDataFlow execute() [" + configuration.getBeanName() + "]");
        runDataFlow();
    }

    private void runDataFlow() {
        DataFlowConfiguration dataFlowConfig = setupDataFlow();
        AppSubmission submission = dataFlowApiProxy.submitDataFlowExecution(dataFlowConfig);
        waitForAppId(submission.getApplicationIds().get(0), configuration.getMicroServiceHostPort());
    }

    private DataFlowConfiguration setupDataFlow() {
        DataFlowConfiguration dataFlowConfig = new DataFlowConfiguration();
        dataFlowConfig.setTargetTableName(configuration.getTargetTableName());
        dataFlowConfig.setCustomerSpace(configuration.getCustomerSpace());
        dataFlowConfig.setDataFlowBeanName(configuration.getBeanName());
        dataFlowConfig.setDataSources(createDataFlowSources());
        dataFlowConfig.setDataFlowParameters(configuration.getDataFlowParams());
        return dataFlowConfig;
    }

    private List<DataFlowSource> createDataFlowSources() {
        List<Table> tables = retrieveRegisteredTablesAndExtraSources();
        List<DataFlowSource> sources = new ArrayList<>();
        for (Table table : tables) {
            DataFlowSource source = new DataFlowSource();
            source.setName(table.getName());
            sources.add(source);
        }
        return sources;
    }

    private List<Table> retrieveRegisteredTablesAndExtraSources() {
        Set<String> tableSet = new HashSet<>(metadataProxy.getTableNames(configuration.getCustomerSpace().toString()));
        List<Table> tables = new ArrayList<>();

        for (String tableName : tableSet) {
            Table t = new Table();
            t.setName(tableName);
            tables.add(t);
        }

        for (String extraSourceName : configuration.getExtraSources().keySet()) {
            if (tableSet.contains(extraSourceName)) {
                continue;
            }
            Table extraSourceTable = MetadataConverter.getTable(yarnConfiguration,
                    configuration.getExtraSources().get(extraSourceName), null, null);
            extraSourceTable.setName(extraSourceName);
            metadataProxy.createTable(configuration.getCustomerSpace().toString(), extraSourceTable.getName(),
                    extraSourceTable);
            tables.add(extraSourceTable);
        }

        return tables;
    }
}
