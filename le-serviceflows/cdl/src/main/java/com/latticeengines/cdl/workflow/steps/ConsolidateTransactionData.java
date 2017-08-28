package com.latticeengines.cdl.workflow.steps;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateAggregateConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidatePartitionConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ConsolidateTransactionDataStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;

@Component("consolidateTransactionData")
public class ConsolidateTransactionData extends ConsolidateDataBase<ConsolidateTransactionDataStepConfiguration> {

    private static final String AGGREGATE_SUFFIX = "_Aggregate";
    private static final String AGGREGATE_TABLE_KEY = "Aggregate";
    private static final String MASTER_TABLE_KEY = "Master";
    private static final String DELTA_TABLE_KEY = "Delta";

    private static final Logger log = LoggerFactory.getLogger(ConsolidateTransactionData.class);

    private static String regex = "^" + TableRoleInCollection.ConsolidatedTransaction.name()
            + "_(?:[0-9]{2})?[0-9]{2}-[0-3]?[0-9]-[0-3]?[0-9]$";
    private static Pattern pattern = Pattern.compile(regex);

    private String srcIdField;

    private int inputMergeStep;
    private int partitionAndAggregateStep;
    private int bucketStep;

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        srcIdField = configuration.getIdField();
    }

    public PipelineTransformationRequest getConsolidateRequest() {
        try {

            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("ConsolidatePipeline");

            inputMergeStep = 0;
            partitionAndAggregateStep = 1;
            bucketStep = 2;
            TransformationStepConfig inputMerge = mergeInputs();
            TransformationStepConfig partitionAggr = partitionAndAggregate();
            TransformationStepConfig bucket = bucket();

            List<TransformationStepConfig> steps = new ArrayList<>();
            steps.add(inputMerge);
            steps.add(partitionAggr);
            if (isBucketing()) {
                steps.add(bucket);
            }
            request.setSteps(steps);
            return request;

        } catch (Exception e) {
            log.error("Failed to run consolidate data pipeline!", e);
            throw new RuntimeException(e);
        }
    }

    private TransformationStepConfig mergeInputs() {
        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = inputTableNames;
        step1.setBaseSources(baseSources);

        Map<String, SourceTable> baseTables = new HashMap<>();
        for (String inputTableName : inputTableNames) {
            baseTables.put(inputTableName, new SourceTable(inputTableName, customerSpace));
        }
        step1.setBaseTables(baseTables);
        step1.setTransformer("consolidateDataTransformer");
        step1.setConfiguration(getConsolidateDataConfig());
        return step1;
    }

    private TransformationStepConfig partitionAndAggregate() {
        /* Step 2: Partition and Aggregate */
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setInputSteps(Collections.singletonList(inputMergeStep));
        step2.setTransformer(DataCloudConstants.TRANSFORMER_CONSOLIDATE_PARTITION);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(servingStoreTablePrefix + AGGREGATE_SUFFIX);
        targetTable.setPrimaryKey(servingStorePrimaryKey);
        step2.setTargetTable(targetTable);
        step2.setConfiguration(getPartitionConfig());
        return step2;
    }

    private TransformationStepConfig bucket() {
        if (!isBucketing()) {
            return null;
        }
        TransformationStepConfig step = new TransformationStepConfig();
        String tableSourceName = "CustomerProfile";
        SourceTable sourceTable = new SourceTable(profileTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);
        // consolidate diff
        step.setInputSteps(Collections.singletonList(partitionAndAggregateStep));
        step.setTransformer(TRANSFORMER_BUCKETER);
        step.setConfiguration(emptyStepConfig(heavyEngineConfig()));
        return step;
    }

    private String getPartitionConfig() {
        ConsolidatePartitionConfig config = new ConsolidatePartitionConfig();
        config.setNamePrefix(batchStoreTablePrefix);
        config.setTimeField("TransactionTime");
        config.setConsolidateDateConfig(getConsolidateDataConfig());
        config.setAggregateConfig(getAggregateConfig());
        return appendEngineConf(config, lightEngineConfig());
    }

    private String getAggregateConfig() {
        ConsolidateAggregateConfig config = new ConsolidateAggregateConfig();
        config.setCountField("Quantity");
        config.setSumField("Amount");
        config.setTrxDateField("TransactionDate");
        config.setGoupByFields(Arrays.asList("AccountId", "ContactId", "EntityType", "ProductName", "TransactionType",
                "TransactionDate"));
        return appendEngineConf(config, lightEngineConfig());
    }

    private String getConsolidateDataConfig() {
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setSrcIdField(srcIdField);
        config.setMasterIdField(TableRoleInCollection.ConsolidatedTransaction.getPrimaryKey().name());
        config.setCreateTimestampColumn(true);
        config.setColumnsFromRight(new HashSet<String>(Arrays.asList("CREATION_DATE")));
        config.setCompositeKeys(Arrays.asList("AccountId", "ContactId", "EntityType", "ProductName", "TransactionType",
                "TransactionTime"));
        return JsonUtils.serialize(config);
    }

    @Override
    public boolean isBucketing() {
        return Boolean.TRUE.equals(isActive);
    }

    @Override
    protected void onPostTransformationCompleted() {
        String aggrTableName = TableUtils.getFullTableName(servingStoreTablePrefix + AGGREGATE_SUFFIX, pipelineVersion);
        Table aggregateTable = metadataProxy.getTable(customerSpace.toString(), aggrTableName);
        putObjectInContext(AGGREGATE_TABLE_KEY, aggregateTable);

        Table masterTable = setupMasterTable(aggregateTable);
        putObjectInContext(MASTER_TABLE_KEY, masterTable);
        Table deltaTable = setupDeltaTable(aggregateTable);
        putObjectInContext(DELTA_TABLE_KEY, deltaTable);

        super.onPostTransformationCompleted();
    }

    private Table setupMasterTable(Table aggregateTable) {
        try {
            List<String> dateFiles = getDateFiles(aggregateTable);
            Table masterTable = createTable(dateFiles, MASTER_TABLE_KEY);
            return masterTable;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private Table setupDeltaTable(Table aggregateTable) {
        try {
            Set<String> dates = getDeltaDates(aggregateTable);
            List<String> dateFiles = getDeltaDateFiles(aggregateTable, dates);
            Table deltaTable = createTable(dateFiles, DELTA_TABLE_KEY);
            return deltaTable;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private Set<String> getDeltaDates(Table aggregateTable) {
        String hdfsPath = aggregateTable.getExtracts().get(0).getPath();
        Set<String> dates = new HashSet<>();
        Iterator<GenericRecord> iter = AvroUtils.iterator(yarnConfiguration, hdfsPath);
        while (iter.hasNext()) {
            GenericRecord record = iter.next();
            String date = record.get("TransactionDate").toString();
            if (!dates.contains(date))
                dates.add(date);
        }
        return dates;
    }

    private List<String> getDeltaDateFiles(Table aggregateTable, Set<String> dates) {
        String hdfsDir = getTableBaseDir(aggregateTable);
        List<String> files = new ArrayList<>();
        for (String date : dates) {
            files.add(hdfsDir + "/" + date);
        }
        return files;
    }

    private List<String> getDateFiles(Table aggregateTable) throws IOException {
        String hdfsDir = getTableBaseDir(aggregateTable);
        List<String> dateFiles = HdfsUtils.getFilesForDir(yarnConfiguration, hdfsDir, new HdfsFilenameFilter() {
            @Override
            public boolean accept(String fileName) {
                Matcher matcher = pattern.matcher(fileName);
                return matcher.matches();
            }
        });
        return dateFiles;
    }

    private String getTableBaseDir(Table aggregateTable) {
        String hdfsPath = aggregateTable.getExtracts().get(0).getPath();
        int index = 0;
        if (hdfsPath.endsWith("*.avro")) {
            index = StringUtils.lastOrdinalIndexOf(hdfsPath, "/", 2);
        } else {
            index = StringUtils.lastOrdinalIndexOf(hdfsPath, "/", 1);
        }
        String hdfsDir = hdfsPath.substring(0, index);
        return hdfsDir;
    }

    private Table createTable(List<String> dateFiles, String tableName) {
        Table table = new Table();
        String fullTableName = TableUtils.getFullTableName(tableName, pipelineVersion);
        table.setName(fullTableName);
        for (String file : dateFiles) {
            addExtract(table, file);
        }
        return table;
    }

    private void addExtract(Table masterTable, String file) {
        Extract extract = new Extract();
        extract.setName("extract");
        extract.setPath(file + "/*.avro");
        extract.setExtractionTimestamp(DateTime.now().getMillis());
        masterTable.addExtract(extract);
    }
}
