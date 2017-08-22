package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.ConsolidatePartitionTransformer.TRANSFORMER_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_CONSOLIDATE_PARTITION;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.TableUtils;
import com.latticeengines.datacloud.dataflow.transformation.Profile;
import com.latticeengines.datacloud.etl.transformation.service.TransformerService;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.dataflow.ConsolidatePartitionParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidatePartitionConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.Table;

@Component(TRANSFORMER_NAME)
public class ConsolidatePartitionTransformer extends
        AbstractDataflowTransformer<ConsolidatePartitionConfig, ConsolidatePartitionParameters> {
    private static final Logger log = LoggerFactory.getLogger(ConsolidatePartitionTransformer.class);

    public static final String TRANSFORMER_NAME = TRANSFORMER_CONSOLIDATE_PARTITION;

    @Autowired
    private TransformerService transformerService;

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Override
    protected String getDataFlowBeanName() {
        return Profile.BEAN_NAME;
    }

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return ConsolidatePartitionConfig.class;
    }

    @Override
    protected Class<ConsolidatePartitionParameters> getDataFlowParametersClass() {
        return ConsolidatePartitionParameters.class;
    }

    @Override
    protected Table executeDataFlow(String workflowDir, TransformStep step, ConsolidatePartitionParameters parameters) {
        String sourceName = parameters.getBaseTables().get(0);
        Table inputTable = step.getBaseTables().get(sourceName);
        Table table = partitionTable(inputTable, workflowDir, step, parameters);

        return table;
    }

    private Table partitionTable(Table inputTable, String workflowDir, TransformStep step,
            ConsolidatePartitionParameters parameters) {

        Source source = step.getBaseSources()[0];
        TableSource tableSource = (TableSource) source;

        String avroDir = inputTable.getExtracts().get(0).getPath();
        Map<String, String> dateFileMap = new HashMap<>();
        Map<String, String> dateTempFileMap = new HashMap<>();
        Map<String, List<GenericRecord>> dateRecordMap = new HashMap<>();
        Set<String> newDates = new HashSet<>();
        try {
            ConsolidatePartitionConfig config = getConfiguration(parameters.getConfJson());

            writeDateFiles(workflowDir, config, tableSource, dateFileMap, dateTempFileMap, dateRecordMap, newDates,
                    avroDir);
            Map<String, Table> dateTableMap = createDateTables(config, dateTempFileMap);
            mergeWithExistingFiles(workflowDir, step, parameters, config, tableSource, newDates, dateFileMap,
                    dateTempFileMap, dateTableMap);

            Table table = aggregateFiles(workflowDir, config, step, parameters, tableSource, newDates, dateFileMap,
                    dateTempFileMap, dateTableMap);

            Table result = moveAggregateFiles(workflowDir, config, step, tableSource, table);
            moveDateFiles(workflowDir, config, step, tableSource, newDates, dateFileMap, dateTempFileMap);
            return result;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void moveDateFiles(String workflowDir, ConsolidatePartitionConfig config, TransformStep step,
            TableSource tableSource, Set<String> newDates, Map<String, String> dateFileMap,
            Map<String, String> dateTempFileMap) throws IOException {
        for (Map.Entry<String, String> entry : dateFileMap.entrySet()) {
            String date = entry.getKey();
            String localDir = dateTempFileMap.get(date);
            String targetDir = dateFileMap.get(date);
            if (HdfsUtils.fileExists(yarnConfiguration, targetDir)) {
                HdfsUtils.rmdir(yarnConfiguration, targetDir);
            }
            HdfsUtils.moveGlobToDir(yarnConfiguration, localDir + "/*.avro", targetDir);
            HdfsUtils.rmdir(yarnConfiguration, localDir);
            if (!newDates.contains(date)) {
                HdfsUtils.rmdir(yarnConfiguration, workflowDir + "/" + date);
            }
        }
    }

    private Map<String, Table> createDateTables(ConsolidatePartitionConfig config, Map<String, String> dateTempFileMap) {
        Map<String, Table> dateTableMap = new HashMap<>();
        for (String date : dateTempFileMap.keySet()) {
            String fullTableName = TableSource.getFullTableName(config.getNamePrefix(), date);
            Table table = TableUtils.createTable(yarnConfiguration, fullTableName, dateTempFileMap.get(date));
            dateTableMap.put(date, table);
        }
        return dateTableMap;
    }

    private Table aggregateFiles(String workflowDir, ConsolidatePartitionConfig config, TransformStep step,
            ConsolidatePartitionParameters parameters, TableSource tableSource, Set<String> newDates,
            Map<String, String> dateFileMap, Map<String, String> dateTempFileMap, Map<String, Table> dateTableMap)
            throws IOException {

        TransformStep newStep = new TransformStep(step.getName() + "_Aggregate");
        newStep.setBaseTables(dateTableMap);
        ArrayList<String> tableNames = new ArrayList<>(dateTableMap.keySet());

        TransformationFlowParameters newParameter = new TransformationFlowParameters();
        newParameter.setBaseTables(tableNames);
        newParameter.setConfJson(config.getAggregateConfig());
        newParameter.setEngineConfiguration(parameters.getEngineConfiguration());

        setTargetTemplate(tableSource, newStep, tableSource.getTable().getName());
        String localAggrDir = getLocalWorkflowDir(workflowDir);
        Table result = dataFlowService.executeDataFlow(newStep, "consolidateAggregateFlow", newParameter, localAggrDir);
        return result;

    }

    private Table moveAggregateFiles(String workflowDir, ConsolidatePartitionConfig config, TransformStep step,
            TableSource tableSource, Table table) throws IOException {
        String targetTableName = TableSource.getFullTableName(config.getNamePrefix() + "_Aggregate",
                step.getTargetVersion());
        String localAggrDir = getLocalWorkflowDir(workflowDir);
        HdfsUtils.moveGlobToDir(yarnConfiguration, localAggrDir + "/*.avro", workflowDir);
        HdfsUtils.rmdir(yarnConfiguration, localAggrDir);
        Table newTable = TableUtils.createTable(yarnConfiguration, targetTableName, workflowDir);
        table.setExtracts(newTable.getExtracts());
        return table;
    }

    private String getLocalWorkflowDir(String workflowDir) {
        return workflowDir + "/aggregate";
    }

    private void setTargetTemplate(TableSource tableSource, TransformStep newStep, String sourceName) {
        Table newTable = new Table();
        newTable.setName(sourceName);
        TableSource targetTemplate = new TableSource(newTable, tableSource.getCustomerSpace());
        newStep.setTargetTemplate(targetTemplate);
    }

    private void writeDateFiles(String workflowDir, ConsolidatePartitionConfig config, TableSource tableSource,
            Map<String, String> dateFileMap, Map<String, String> dateTempFileMap,
            Map<String, List<GenericRecord>> dateRecordMap, Set<String> newDates, String avroDir) throws IOException {

        Iterator<GenericRecord> iter = AvroUtils.iterator(yarnConfiguration, avroDir);
        Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroDir);
        while (iter.hasNext()) {
            GenericRecord record = iter.next();
            String value = record.get(config.getTimeField()).toString();
            Long time = Long.valueOf(value);
            String date = HdfsPathBuilder.dateOnlyFormat.format(new Date(time * 1000));
            populateDateMaps(workflowDir, config, tableSource, dateFileMap, dateTempFileMap, newDates, date);
            if (!dateRecordMap.containsKey(date)) {
                dateRecordMap.put(date, new ArrayList<>());
            }
            dateRecordMap.get(date).add(record);
            if (dateRecordMap.get(date).size() >= 20) {
                writeDataBuffer(schema, date, dateTempFileMap, dateRecordMap);
            }
        }
        log.info("New Dates=" + newDates);
        for (Map.Entry<String, List<GenericRecord>> entry : dateRecordMap.entrySet()) {
            writeDataBuffer(schema, entry.getKey(), dateTempFileMap, dateRecordMap);
        }
    }

    private void mergeWithExistingFiles(String workflowDir, TransformStep step,
            ConsolidatePartitionParameters parameters, ConsolidatePartitionConfig config, TableSource tableSource,
            Set<String> newDates, Map<String, String> dateFileMap, Map<String, String> dateTempFileMap,
            Map<String, Table> dateTableMap) {

        for (String date : dateFileMap.keySet()) {
            if (newDates.contains(date)) {
                continue;
            }
            mergeFile(step, parameters, config, tableSource, dateFileMap, dateTempFileMap, dateTableMap, date);
        }
    }

    private Table mergeFile(TransformStep step, ConsolidatePartitionParameters parameters,
            ConsolidatePartitionConfig config, TableSource tableSource, Map<String, String> dateFileMap,
            Map<String, String> dateTempFileMap, Map<String, Table> dateTableMap, String date) {

        String fullTableName = TableSource.getFullTableName(config.getNamePrefix(), date);
        Table existingSourceTable = TableUtils.createTable(yarnConfiguration, fullTableName, dateFileMap.get(date));
        Map<String, Table> baseTables = new HashMap<>();
        baseTables.put(date, dateTableMap.get(date));
        baseTables.put(fullTableName, existingSourceTable);

        TransformStep newStep = new TransformStep(getName() + "_" + fullTableName);
        newStep.setBaseTables(baseTables);
        ArrayList<String> tableNames = new ArrayList<>();
        tableNames.add(date);
        tableNames.add(fullTableName);

        TransformationFlowParameters newParameter = new TransformationFlowParameters();
        newParameter.setBaseTables(tableNames);
        newParameter.setConfJson(config.getConsolidateDataConfig());
        newParameter.setEngineConfiguration(parameters.getEngineConfiguration());

        setTargetTemplate(tableSource, newStep, fullTableName);
        String outputDir = dateTempFileMap.get(date) + "_output";
        Table result = dataFlowService.executeDataFlow(newStep, "consolidateDataFlow", newParameter, outputDir);
        dateTableMap.put(date, result);
        dateTempFileMap.put(date, outputDir);

        return result;
    }

    private void populateDateMaps(String workflowDir, ConsolidatePartitionConfig config, TableSource tableSource,
            Map<String, String> dateFileMap, Map<String, String> dateTempFileMap, Set<String> newDates, String date)
            throws IOException {
        if (!dateFileMap.containsKey(date)) {
            String tableName = TableSource.getFullTableName(config.getNamePrefix(), date);
            String hdfsPath = hdfsPathBuilder.constructTablePath(tableName, tableSource.getCustomerSpace(), "")
                    .toString();
            dateFileMap.put(date, hdfsPath);
            if (!newDates.contains(date) && !HdfsUtils.fileExists(yarnConfiguration, hdfsPath)) {
                newDates.add(date);
            }
            dateTempFileMap.put(date, workflowDir + "/" + date);
        }
    }

    private void writeDataBuffer(Schema schema, String date, Map<String, String> dateTempFileMap,
            Map<String, List<GenericRecord>> dateRecordMap) throws IOException {
        List<GenericRecord> records = dateRecordMap.get(date);
        String tempPath = dateTempFileMap.get(date);
        String fileName = tempPath + "/part-00000.avro";
        if (!HdfsUtils.fileExists(yarnConfiguration, fileName)) {
            AvroUtils.writeToHdfsFile(yarnConfiguration, schema, fileName, records);
        } else {
            AvroUtils.appendToHdfsFile(yarnConfiguration, fileName, records);
        }
        records.clear();

    }
}
