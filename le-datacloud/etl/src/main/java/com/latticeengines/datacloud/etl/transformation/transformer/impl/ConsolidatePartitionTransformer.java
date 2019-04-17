package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.ConsolidatePartitionTransformer.TRANSFORMER_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_CONSOLIDATE_PARTITION;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.dataflow.transformation.Profile;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.dataflow.ConsolidatePartitionParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ConsolidatePartitionConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.MetaDataTableUtils;

@Component(TRANSFORMER_NAME)
public class ConsolidatePartitionTransformer
        extends AbstractDataflowTransformer<ConsolidatePartitionConfig, ConsolidatePartitionParameters> {
    private static final Logger log = LoggerFactory.getLogger(ConsolidatePartitionTransformer.class);

    public static final String TRANSFORMER_NAME = TRANSFORMER_CONSOLIDATE_PARTITION;

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
        Set<String> newDates = new HashSet<>();
        try {
            ConsolidatePartitionConfig config = getConfiguration(parameters.getConfJson());

            writeDateFiles(workflowDir, config, tableSource, dateFileMap, dateTempFileMap, newDates, avroDir);
            Table targetTable = createTargetDateTable(config, dateTempFileMap, newDates, tableSource);
            Table outputTable = mergeWithExistingFiles(workflowDir, inputTable, parameters, config, tableSource,
                    dateTempFileMap, targetTable);
            Table aggrTable = aggregateFiles(workflowDir, config, step, parameters, tableSource, newDates,
                    dateTempFileMap, outputTable);
            Table result = moveAggregateFiles(workflowDir, config, step, tableSource, aggrTable);
            moveDateAggrFiles(workflowDir, config, step, tableSource, newDates, dateFileMap, dateTempFileMap);
            moveDateFiles(workflowDir, config, step, tableSource, newDates, dateFileMap, dateTempFileMap);
            return result;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void moveDateAggrFiles(String workflowDir, ConsolidatePartitionConfig config, TransformStep step,
            TableSource tableSource, Set<String> newDates, Map<String, String> dateFileMap,
            Map<String, String> dateTempFileMap) throws IOException {
        for (Map.Entry<String, String> entry : dateFileMap.entrySet()) {
            String date = entry.getKey();
            String localDir = getDateAggrDir(dateTempFileMap, date);
            String tableName = TableSource.getFullTableName(config.getAggrNamePrefix(), date);
            String hdfsPath = hdfsPathBuilder.constructTablePath(tableName, tableSource.getCustomerSpace(), "")
                    .toString();
            String targetDir = hdfsPath;
            if (HdfsUtils.fileExists(yarnConfiguration, targetDir)) {
                HdfsUtils.rmdir(yarnConfiguration, targetDir);
            }
            HdfsUtils.moveGlobToDir(yarnConfiguration, localDir + "/*.avro", targetDir);
            HdfsUtils.rmdir(yarnConfiguration, localDir);
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

    private Table createTargetDateTable(ConsolidatePartitionConfig config, Map<String, String> dateTempFileMap,
            Set<String> newDates, TableSource tableSource) {
        List<String> existingTables = new ArrayList<>();
        dateTempFileMap.forEach((k, v) -> {
            if (!newDates.contains(k)) {
                existingTables.add(TableSource.getFullTableName(config.getNamePrefix(), k));
            }
        });
        if (CollectionUtils.isEmpty(existingTables)) {
            return null;
        }
        String targetDates = "{" + String.join(",", existingTables) + "}";
        String hdfsPath = PathBuilder
                .buildDataTablePath(HdfsPodContext.getHdfsPodId(), tableSource.getCustomerSpace(), "").toString();
        String fullTableName = config.getNamePrefix() + "_target";
        Table table = new Table();
        Extract extract = new Extract();
        table.setName(fullTableName);
        extract.setName("extract_traget");
        extract.setPath(hdfsPath + "/" + targetDates + "/*.avro");
        table.setExtracts(Collections.singletonList(extract));
        return table;
    }

    private Table aggregateFiles(String workflowDir, ConsolidatePartitionConfig config, TransformStep step,
            ConsolidatePartitionParameters parameters, TableSource tableSource, Set<String> newDates,
            Map<String, String> dateTempFileMap, Table outputTable) throws IOException {

        TransformStep newStep = new TransformStep(step.getName() + "_Aggregate");
        Map<String, Table> baseTables = new HashMap<>();
        baseTables.put(outputTable.getName(), outputTable);
        newStep.setBaseTables(baseTables);
        List<String> tableNames = Arrays.asList(outputTable.getName());

        TransformationFlowParameters newParameter = new TransformationFlowParameters();
        newParameter.setBaseTables(tableNames);
        newParameter.setConfJson(config.getAggregateConfig());
        newParameter.setEngineConfiguration(parameters.getEngineConfiguration());

        setTargetTemplate(tableSource, newStep, tableSource.getTable().getName());
        String localAggrDir = getLocalAggrDir(workflowDir);
        Table result = dataFlowService.executeDataFlow(newStep, "consolidateAggregateFlow", newParameter, localAggrDir);
        Map<String, String> dateFileMap = getAggrFileMap(dateTempFileMap);
        distributeDateFile(config, dateFileMap, result, true);
        return result;

    }

    private Table distributeDateFile(ConsolidatePartitionConfig config, Map<String, String> dateFileMap, Table result,
            boolean isDate) throws IOException {
        String avroDir = result.getExtracts().get(0).getPath() + "/*.avro";
        Iterator<GenericRecord> iter = AvroUtils.iterator(yarnConfiguration, avroDir);
        Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroDir);
        Map<String, List<GenericRecord>> dateRecordMap = new HashMap<>();
        while (iter.hasNext()) {
            GenericRecord record = iter.next();
            String date = null;
            if (isDate) {
                date = record.get(config.getTrxDateField()).toString();
            } else {
                String value = record.get(config.getTimeField()).toString();
                date = DateTimeUtils.toDateOnlyFromMillis(value);
            }
            if (!dateRecordMap.containsKey(date)) {
                dateRecordMap.put(date, new ArrayList<>());
            }
            dateRecordMap.get(date).add(record);
            if (dateRecordMap.get(date).size() >= 100) {
                writeDataBuffer(schema, date, dateFileMap, dateRecordMap);
            }
        }
        for (Map.Entry<String, List<GenericRecord>> entry : dateRecordMap.entrySet()) {
            writeDataBuffer(schema, entry.getKey(), dateFileMap, dateRecordMap);
        }

        return result;
    }

    private Map<String, String> getAggrFileMap(Map<String, String> dateTempFileMap) {
        Map<String, String> dateAggrFileMap = new HashMap<>();
        dateTempFileMap.forEach((k, v) -> dateAggrFileMap.put(k, getDateAggrDir(dateTempFileMap, k)));
        return dateAggrFileMap;
    }

    private String getDateAggrDir(Map<String, String> dateTempFileMap, String date) {
        return dateTempFileMap.get(date) + "_aggregate";
    }

    private String getOutputDir(String workflowDir) {
        return workflowDir + "/output";
    }

    private String getLocalAggrDir(String workflowDir) {
        return workflowDir + "/aggregate";
    }

    private Table moveAggregateFiles(String workflowDir, ConsolidatePartitionConfig config, TransformStep step,
            TableSource tableSource, Table table) throws IOException {
        String targetTableName = TableSource.getFullTableName(config.getNamePrefix() + "_Aggregate",
                step.getTargetVersion());
        String localAggrDir = getLocalAggrDir(workflowDir);
        HdfsUtils.moveGlobToDir(yarnConfiguration, localAggrDir + "/*.avro", workflowDir);
        HdfsUtils.rmdir(yarnConfiguration, localAggrDir);
        Table newTable = MetaDataTableUtils.createTable(yarnConfiguration, targetTableName, workflowDir);
        table.setExtracts(newTable.getExtracts());

        return table;
    }

    private void setTargetTemplate(TableSource tableSource, TransformStep newStep, String sourceName) {
        Table newTable = new Table();
        newTable.setName(sourceName);
        TableSource targetTemplate = new TableSource(newTable, tableSource.getCustomerSpace());
        newStep.setTargetTemplate(targetTemplate);
    }

    private void writeDateFiles(String workflowDir, ConsolidatePartitionConfig config, TableSource tableSource,
            Map<String, String> dateFileMap, Map<String, String> dateTempFileMap, Set<String> newDates, String avroDir)
            throws IOException {
        Iterator<GenericRecord> iter = AvroUtils.iterator(yarnConfiguration, avroDir);
        while (iter.hasNext()) {
            GenericRecord record = iter.next();
            String value = record.get(config.getTimeField()).toString();
            String date = DateTimeUtils.toDateOnlyFromMillis(value);
            populateDateMaps(workflowDir, config, tableSource, dateFileMap, dateTempFileMap, newDates, date);
        }
        log.info("New Dates=" + newDates);
    }

    private Table mergeWithExistingFiles(String workflowDir, Table inputTable,
            ConsolidatePartitionParameters parameters, ConsolidatePartitionConfig config, TableSource tableSource,
            Map<String, String> dateTempFileMap, Table targetTable) throws IOException {

        Map<String, Table> baseTables = new HashMap<>();
        baseTables.put(inputTable.getName(), inputTable);
        if (targetTable != null)
            baseTables.put(targetTable.getName(), targetTable);

        TransformStep newStep = new TransformStep(getName() + "_" + config.getNamePrefix());
        newStep.setBaseTables(baseTables);
        ArrayList<String> tableNames = new ArrayList<>();
        tableNames.add(inputTable.getName());
        if (targetTable != null)
            tableNames.add(targetTable.getName());

        TransformationFlowParameters newParameter = new TransformationFlowParameters();
        newParameter.setBaseTables(tableNames);
        newParameter.setConfJson(config.getConsolidateDataConfig());
        newParameter.setEngineConfiguration(parameters.getEngineConfiguration());

        setTargetTemplate(tableSource, newStep, config.getNamePrefix() + "_output");
        String outputDir = getOutputDir(workflowDir);
        Table result = dataFlowService.executeDataFlow(newStep, "consolidateDataFlow", newParameter, outputDir);
        distributeDateFile(config, dateTempFileMap, result, false);
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

    private void writeDataBuffer(Schema schema, String date, Map<String, String> dateFileMap,
            Map<String, List<GenericRecord>> dateRecordMap) throws IOException {
        List<GenericRecord> records = dateRecordMap.get(date);
        String tempPath = dateFileMap.get(date);
        String fileName = tempPath + "/part-00000.avro";
        if (!HdfsUtils.fileExists(yarnConfiguration, fileName)) {
            AvroUtils.writeToHdfsFile(yarnConfiguration, schema, fileName, records);
        } else {
            AvroUtils.appendToHdfsFile(yarnConfiguration, fileName, records);
        }
        records.clear();

    }
}
