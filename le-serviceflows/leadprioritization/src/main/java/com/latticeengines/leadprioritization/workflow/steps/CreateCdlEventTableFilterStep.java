package com.latticeengines.leadprioritization.workflow.steps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.dataflow.CreateCdlEventTableFilterParameters;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.CreateCdlEventTableFilterConfiguration;
import com.latticeengines.domain.exposed.util.MetaDataTableUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.objectapi.EventProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("createCdlEventTableFilterStep")
public class CreateCdlEventTableFilterStep extends RunDataFlow<CreateCdlEventTableFilterConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(CreateCdlEventTableFilterStep.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private EventProxy eventProxy;

    private Table trainFilterTable;
    private Table targetFilterTable;

    @Override
    public void onConfigurationInitialized() {
        CreateCdlEventTableFilterConfiguration configuration = getConfiguration();
        configuration.setDataFlowParams(createDataFlowParameters());
    }

    private DataFlowParameters createDataFlowParameters() {
        trainFilterTable = getTrainFilterTable();
        targetFilterTable = getTargetFilterTable();
        CreateCdlEventTableFilterParameters parameters = new CreateCdlEventTableFilterParameters(
                trainFilterTable.getName(), targetFilterTable.getName());
        return parameters;
    }

    private Table getTrainFilterTable() {
        Table trainFilterTable = null;
        if (StringUtils.isNotBlank(configuration.getTrainFilterTableName())) {
            trainFilterTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                    configuration.getTrainFilterTableName());
            if (trainFilterTable != null) {
                return trainFilterTable;
            }
        }
        Schema schema = getTrainSchema();
        String filePath = PathBuilder
                .buildDataTablePath(CamilleEnvironment.getPodId(), configuration.getCustomerSpace()).toString();
        String tableName = configuration.getTargetTableName() + "_train_filter";
        filePath += "/" + tableName + "/" + "/part-00000.avro";
        return convertToTable(schema, tableName, filePath, configuration.getTrainQuery(), InterfaceName.Train);
    }

    private Table getTargetFilterTable() {
        Table targetFilterTable = null;
        if (StringUtils.isNotBlank(configuration.getTargetFilterTableName())) {
            targetFilterTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                    configuration.getTargetFilterTableName());
            if (targetFilterTable != null) {
                return targetFilterTable;
            }
        }
        Schema schema = getTargetSchema();
        String filePath = PathBuilder
                .buildDataTablePath(CamilleEnvironment.getPodId(), configuration.getCustomerSpace()).toString();
        String tableName = configuration.getTargetTableName() + "_target_filter";
        filePath += "/" + tableName + "/" + "/part-00000.avro";
        targetFilterTable = convertToTable(schema, tableName, filePath, configuration.getTargetQuery(),
                InterfaceName.Target);
        return targetFilterTable;
    }

    private Table convertToTable(Schema schema, String tableName, String filePath, FrontEndQuery query,
            InterfaceName type) {

        int rowNumber = 0, pageSize = 10000;
        long total = 0;
        while (true) {
            query.setPageFilter(new PageFilter(rowNumber, pageSize));
            DataPage dataPage = null;
            switch (type) {
            case Train:
                dataPage = eventProxy.getTrainingTuples(configuration.getCustomerSpace().toString(), query);
                break;
            case Target:
                dataPage = eventProxy.getEventTuples(configuration.getCustomerSpace().toString(), query);
                break;
            default:
                dataPage = eventProxy.getScoringTuples(configuration.getCustomerSpace().toString(), query);
            }

            List<Map<String, Object>> rows = dataPage.getData();
            if (CollectionUtils.isEmpty(rows)) {
                break;
            }
            List<GenericRecord> records = new ArrayList<>();
            for (Map<String, Object> row : rows) {
                GenericRecord record = new GenericData.Record(schema);
                record.put(InterfaceName.AccountId.name(), row.get(InterfaceName.AccountId.name()));
                record.put(InterfaceName.PeriodId.name(),
                        Long.valueOf(row.get(InterfaceName.PeriodId.name()).toString()));
                records.add(record);
            }
            writeRecords(schema, filePath, records);
            total += rows.size();
            rowNumber += pageSize;
        }
        log.info(type + "total filter rows=" + total);
        Table table = MetaDataTableUtils.createTable(yarnConfiguration, tableName, filePath);
        table.getExtracts().get(0).setExtractionTimestamp(System.currentTimeMillis());
        return table;
    }

    private void writeRecords(Schema schema, String targetFile, List<GenericRecord> data) {
        try {
            if (!HdfsUtils.fileExists(yarnConfiguration, targetFile)) {
                AvroUtils.writeToHdfsFile(yarnConfiguration, schema, targetFile, data);
            } else {
                AvroUtils.appendToHdfsFile(yarnConfiguration, targetFile, data);
            }
            log.info("Write a buffer of " + data.size() + " rows to " + targetFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Schema getTrainSchema() {
        String schemaString = "{\"namespace\": \"RatingEngineModel\", \"type\": \"record\", "
                + "\"name\": \"RatingEngineModelTrainFilter\"," + "\"fields\": ["
                + "{\"name\": \"AccountId\", \"type\": \"string\"}, {\"name\": \"PeriodId\", \"type\": \"long\"}"
                + "]}";
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }

    private Schema getTargetSchema() {
        String schemaString = "{\"namespace\": \"RatingEngineModel\", \"type\": \"record\", "
                + "\"name\": \"RatingEngineModelTargetFilter\"," + "\"fields\": ["
                + "{\"name\": \"AccountId\", \"type\": \"string\"}, {\"name\": \"PeriodId\", \"type\": \"long\"}"
                + "]}";
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }

    @Override
    public void onExecutionCompleted() {
        Table filterTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getTargetTableName());
        putObjectInContext(FILTER_EVENT_TABLE, filterTable);
        metadataProxy.deleteTable(configuration.getCustomerSpace().toString(), trainFilterTable.getName());
        metadataProxy.deleteTable(configuration.getCustomerSpace().toString(), targetFilterTable.getName());
    }

}
