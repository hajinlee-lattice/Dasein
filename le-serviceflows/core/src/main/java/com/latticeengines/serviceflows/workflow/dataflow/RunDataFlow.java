package com.latticeengines.serviceflows.workflow.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataflow.DataFlowConfiguration;
import com.latticeengines.domain.exposed.dataflow.DataFlowSource;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

public class RunDataFlow<T extends DataFlowStepConfiguration> extends BaseWorkflowStep<T> {

    private static final Log log = LogFactory.getLog(RunDataFlow.class);

    @Override
    public void execute() {
        log.info("Inside RunDataFlow execute()");

        runDataFlow();
    }

    private void runDataFlow() {
        DataFlowConfiguration dataFlowConfig = setupDataFlow();
        String url = configuration.getMicroServiceHostPort() + "/dataflowapi/dataflows/";

        AppSubmission submission = restTemplate.postForObject(url, dataFlowConfig, AppSubmission.class);
        waitForAppId(submission.getApplicationIds().get(0).toString(), configuration.getMicroServiceHostPort());
    }

    private DataFlowConfiguration setupDataFlow() {
        DataFlowConfiguration dataFlowConfig = new DataFlowConfiguration();
        dataFlowConfig.setName(configuration.getName());
        dataFlowConfig.setCustomerSpace(configuration.getCustomerSpace());
        dataFlowConfig.setDataFlowBeanName(configuration.getBeanName());
        dataFlowConfig.setDataSources(createDataFlowSources());
        dataFlowConfig.setTargetPath(configuration.getTargetPath());
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

    @SuppressWarnings("unchecked")
    private List<Table> retrieveRegisteredTablesAndExtraSources() {
        String url = String.format("%s/metadata/customerspaces/%s/tables", configuration.getMicroServiceHostPort(),
                configuration.getCustomerSpace());
        List<String> tableList = restTemplate.getForObject(url, List.class);
        List<Table> tables = new ArrayList<>();

        for (String tableName : tableList) {
            Table t = new Table();
            t.setName(tableName);
            tables.add(t);
        }

        for (String extraSourceName : configuration.getExtraSources().keySet()) {
            Table extraSourceTable = MetadataConverter.getTable(yarnConfiguration,
                    configuration.getExtraSources().get(extraSourceName), null, null);
            extraSourceTable.setName(extraSourceName);
            // register the extra source table
            url = String.format("%s/metadata/customerspaces/%s/tables/%s", configuration.getMicroServiceHostPort(),
                    configuration.getCustomerSpace(), extraSourceTable.getName());
            restTemplate.postForLocation(url, extraSourceTable);
            tables.add(extraSourceTable);
        }

        return tables;
    }

}
