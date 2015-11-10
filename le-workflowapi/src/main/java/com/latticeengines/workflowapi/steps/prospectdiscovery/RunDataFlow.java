package com.latticeengines.workflowapi.steps.prospectdiscovery;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataflow.DataFlowConfiguration;
import com.latticeengines.domain.exposed.dataflow.DataFlowSource;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.MetadataConverter;

@Component("runDataFlow")
public class RunDataFlow extends BaseFitModelStep<BaseFitModelStepConfiguration> {

    private static final Log log = LogFactory.getLog(RunDataFlow.class);

    @Override
    public void execute() {
        log.info("Inside RunDataFlow execute()");

        runDataFlow();
    }

    private void runDataFlow() {
        DataFlowConfiguration dataFlowConfig = setupPreMatchTableDataFlow();
        String url = configuration.getMicroServiceHostPort() + "/dataflowapi/dataflows/";

        AppSubmission submission = restTemplate.postForObject(url, dataFlowConfig, AppSubmission.class);
        waitForAppId(submission.getApplicationIds().get(0).toString(), configuration.getMicroServiceHostPort());
    }

    private DataFlowConfiguration setupPreMatchTableDataFlow() {
        DataFlowConfiguration dataFlowConfig = new DataFlowConfiguration();
        dataFlowConfig.setName("PrematchFlow");
        dataFlowConfig.setCustomerSpace(CustomerSpace.parse(configuration.getCustomerSpace()));
        dataFlowConfig.setDataFlowBeanName("preMatchEventTableFlow");
        dataFlowConfig.setDataSources(createDataFlowSources());
        dataFlowConfig.setTargetPath("/PrematchFlowRun");
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
        String url = String.format("%s/metadata/customerspaces/%s/tables", configuration.getMicroServiceHostPort(), configuration.getCustomerSpace());
        List<String> tableList = restTemplate.getForObject(url, List.class);
        List<Table> tables = new ArrayList<>();

        for (String tableName : tableList) {
            Table t = new Table();
            t.setName(tableName);
            tables.add(t);
        }

        Table stopList = MetadataConverter.readMetadataFromAvroFile(yarnConfiguration, "/tmp/Stoplist/Stoplist.avro",
                null, null);
        stopList.getExtracts().get(0).setPath("/tmp/Stoplist/*.avro");
        // register the stop list table
        url = String.format("%s/metadata/customerspaces/%s/tables/%s", configuration.getMicroServiceHostPort(), configuration.getCustomerSpace(),
                stopList.getName());
        restTemplate.postForLocation(url, stopList);
        tables.add(stopList);
        return tables;
    }

}
