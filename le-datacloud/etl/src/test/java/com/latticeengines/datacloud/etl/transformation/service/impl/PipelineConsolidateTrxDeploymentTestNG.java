package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateAggregateConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidatePartitionConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransactionAggregateConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.transaction.NamedPeriod;
import com.latticeengines.domain.exposed.metadata.transaction.TransactionMetrics;

public class PipelineConsolidateTrxDeploymentTestNG extends PipelineTransformationDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PipelineConsolidateTrxDeploymentTestNG.class);

    private String tableName1 = "ConsolidateTrxTable1";
    private String tableName2 = "ConsolidateTrxTable2";
    private String tableName3 = "ConsolidateTrxTable3";
    private String mergedTableName1 = "MergedTrxTable1";
    private String mergedTableName2 = "MergedTrxTable2";
    private String aggregatedTableName1 = "AggregateTrxTable1";
    private String aggregatedTableName2 = "AggregateTrxTable2";
    private String historyTableName1 = "TrxHistoryTable1";
    private String historyTableName2 = "TrxHistoryTable2";
    private String accountTableName = "AccountTable1";

    List<String> fieldNames = Arrays.asList("TransactionId", "AccountId", "ContactId", "TransactionType", "ProductId",
            "Amount", "Quantity", "OrderId", "TransactionTime", "ExtensionAttr1");
    List<Class<?>> clz = Arrays.asList((Class<?>) String.class, String.class, String.class, String.class, String.class,
            Double.class, Long.class, String.class, Long.class, String.class);

    List<String> accountFieldNames = Arrays.asList("AccountId");
    List<Class<?>> accountClz = Arrays.asList((Class<?>) String.class);

    private static final CustomerSpace customerSpace = CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE);

    private PipelineTransformationConfiguration currentConfig = null;
    private TableSource targetTableSource = null;
    private HashMap<String, String> productTable = new HashMap<String, String>();

    @BeforeMethod(groups = "deployment")
    public void beforeMethod() {
        prepareCleanPod("PipelineConsolidateDeploymentTestNG");
    }

    @AfterMethod(groups = "deployment")
    public void afterMethod() {

        cleanupProgressTables();

        // cleanup intermediate table
        cleanupRegisteredTable(accountTableName);
        cleanupRegisteredTable(tableName1);
        cleanupRegisteredTable(tableName2);
        cleanupRegisteredTable(tableName3);
        cleanupRegisteredTable(TableSource.getFullTableName(mergedTableName1, targetVersion));

        prepareCleanPod("PipelineConsolidateDeploymentTestNG");
    }

    @Test(groups = "deployment", enabled = true)
    public void testTableToTable() {
        targetVersion = HdfsPathBuilder.dateFormat.format(new Date());

        uploadAndRegisterTableSource(accountTableName, accountTableName, null, null);
        uploadAndRegisterTableSource(tableName1, tableName1, null, null);
        uploadAndRegisterTableSource(tableName2, tableName2, null, null);
        uploadAndRegisterTableSource(tableName3, tableName3, null, null);
        currentConfig = getConcolidateConfig();

        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);

        verifyMergedTable();
        verifyHistoryTable();
        confirmResultFile(progress);

    }

    @Override
    protected String getTargetSourceName() {
        return aggregatedTableName2;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return null;
    }

    @Override
    protected TableSource getTargetTableSource() {
        targetTableSource = convertTargetTableSource(aggregatedTableName2);
        return targetTableSource;
    }

    private TableSource convertTargetTableSource(String tableName) {
        return hdfsSourceEntityMgr.materializeTableSource((tableName + "_" + targetVersion), customerSpace);
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        return currentConfig;
    }

    private PipelineTransformationConfiguration getConcolidateConfig() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("ConsolidatePipeline");
            configuration.setVersion(targetVersion);

            List<TransformationStepConfig> subSteps1 = createSteps(tableName1, tableName2, 0, mergedTableName1,
                    aggregatedTableName1, historyTableName1);
            List<TransformationStepConfig> subSteps2 = createSteps(tableName2, tableName3, 3, mergedTableName2,
                    aggregatedTableName2, historyTableName2);

            /* Final */
            List<TransformationStepConfig> steps = new ArrayList<>();
            steps.addAll(subSteps1);
            steps.addAll(subSteps2);
            configuration.setSteps(steps);

            return configuration;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<TransformationStepConfig> createSteps(String tableName1, String tableName2, int stepInput,
            String mergedTableName, String aggregatedTableName, String historyTableName) {
        /* Step 1: 1st Merge */
        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = Arrays.asList(tableName2, tableName1);
        step1.setBaseSources(baseSources);

        SourceTable sourceTable1 = new SourceTable(tableName1, customerSpace);
        SourceTable sourceTable2 = new SourceTable(tableName2, customerSpace);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableName1, sourceTable1);
        baseTables.put(tableName2, sourceTable2);
        step1.setBaseTables(baseTables);
        step1.setTransformer("consolidateDataTransformer");

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(mergedTableName);
        step1.setTargetTable(targetTable);
        step1.setConfiguration(getConsolidateDataConfig());

        /* Step 2: 1st Partition and Aggregate */
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setInputSteps(Collections.singletonList(stepInput));
        step2.setTransformer(DataCloudConstants.TRANSFORMER_CONSOLIDATE_PARTITION);

        targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(aggregatedTableName);
        step2.setTargetTable(targetTable);
        step2.setConfiguration(getPartitionConfig());

        /* Step 3: calculate purchase history */
        TransformationStepConfig step3 = new TransformationStepConfig();
        step3.setInputSteps(Collections.singletonList(stepInput + 1));
        baseSources = Arrays.asList(accountTableName);
        step3.setBaseSources(baseSources);
        SourceTable accountTable = new SourceTable(accountTableName, customerSpace);
        baseTables = new HashMap<>();
        baseTables.put(accountTableName, accountTable);
        step3.setBaseTables(baseTables);
        step3.setTransformer(DataCloudConstants.TRANSFORMER_TRANSACTION_AGGREGATOR);

        targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(historyTableName);
        step3.setTargetTable(targetTable);
        step3.setConfiguration(getHistoryConfig());

        return Arrays.asList(step1, step2, step3);
    }

    private String getPartitionConfig() {
        ConsolidatePartitionConfig config = new ConsolidatePartitionConfig();
        config.setNamePrefix(TableRoleInCollection.AggregatedTransaction.name());
        config.setAggrNamePrefix(TableRoleInCollection.AggregatedTransaction.name());
        config.setTimeField("TransactionTime");
        config.setTrxDateField("TransactionDate");
        config.setConsolidateDateConfig(getConsolidateDataConfig());
        config.setAggregateConfig(getAggregateConfig());
        return JsonUtils.serialize(config);
    }

    private String getAggregateConfig() {
        ConsolidateAggregateConfig config = new ConsolidateAggregateConfig();
        config.setCountField("Quantity");
        config.setSumField("Amount");
        config.setTrxDateField("TransactionDate");
        config.setGoupByFields(
                Arrays.asList("AccountId", "ContactId", "ProductId", "TransactionType", "TransactionDate"));

        return JsonUtils.serialize(config);
    }

    private String getConsolidateDataConfig() {
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setSrcIdField(TableRoleInCollection.AggregatedTransaction.getPrimaryKey().name());
        config.setMasterIdField(TableRoleInCollection.AggregatedTransaction.getPrimaryKey().name());
        config.setCreateTimestampColumn(true);
        config.setColumnsFromRight(new HashSet<String>(Arrays.asList("CREATION_DATE")));
        config.setCompositeKeys(
                Arrays.asList("AccountId", "ContactId", "ProductId", "TransactionType", "TransactionTime"));
        return JsonUtils.serialize(config);
    }

    private String getHistoryConfig() {
        log.info("ProductTable " + productTable.size());
        TransactionAggregateConfig conf = new TransactionAggregateConfig();
        initProductTable();
        conf.setProductMap(productTable);
        conf.setTransactionType("PurchaseHistory");
        conf.setIdField(InterfaceName.AccountId.name());
        conf.setAccountField(InterfaceName.AccountId.name());
        conf.setProductField(InterfaceName.ProductId.name());
        conf.setTypeField(InterfaceName.TransactionType.name());
        conf.setDateField(InterfaceName.TransactionDate.name());
        conf.setQuantityField(InterfaceName.TotalQuantity.name());
        conf.setAmountField(InterfaceName.TotalAmount.name());

        List<String> periods = new ArrayList<String>();
        List<String> metrics = new ArrayList<String>();

        periods.add(NamedPeriod.HASEVER.getName());
        metrics.add(TransactionMetrics.PURCHASED.getName());
        periods.add(NamedPeriod.LASTQUARTER.getName());
        metrics.add(TransactionMetrics.QUANTITY.getName());
        periods.add(NamedPeriod.LASTQUARTER.getName());
        metrics.add(TransactionMetrics.AMOUNT.getName());
        conf.setPeriods(periods);
        conf.setMetrics(metrics);
        return JsonUtils.serialize(conf);
   }

    private void cleanupRegisteredTable(String tableName) {
        metadataProxy.deleteTable(customerSpace.toString(), tableName);
    }

    private void verifyMergedTable() {
        String mergedTableFullName = TableSource.getFullTableName(mergedTableName1, targetVersion);
        verifyRegisteredTable(mergedTableFullName, fieldNames.size() + 3);
        verifyRecordsInMergedTable(mergedTableFullName);
    }

    private void verifyHistoryTable() {
        String historyTableFullName = TableSource.getFullTableName(historyTableName1, targetVersion);
        verifyRegisteredTable(historyTableFullName, productTable.size() * 3 + 1);
        verifyRecordsInHistoryTable(historyTableFullName);
    }

    private void verifyRecordsInHistoryTable(String mergedTableFullName) {
        log.info("Start to verify records one by one.");
        List<GenericRecord> records = getRecordFromTable(mergedTableFullName);
        Integer rowCount = 0;
        Map<String, GenericRecord> recordMap = new HashMap<>();
        for (GenericRecord record : records) {
            String id = String
                    .valueOf(record.get(TableRoleInCollection.ConsolidatedPurchaseHistory.getPrimaryKey().name()));
            recordMap.put(id, record);
            rowCount++;
        }
        Assert.assertEquals(rowCount, new Integer(5));
    }

    private void verifyRecordsInMergedTable(String mergedTableFullName) {
        log.info("Start to verify records one by one.");
        List<GenericRecord> records = getRecordFromTable(mergedTableFullName);
        Integer rowCount = 0;
        Map<String, GenericRecord> recordMap = new HashMap<>();
        for (GenericRecord record : records) {
            String id = String
                    .valueOf(record.get(TableRoleInCollection.AggregatedTransaction.getPrimaryKey().name()));
            recordMap.put(id, record);
            rowCount++;
        }
        Assert.assertEquals(rowCount, new Integer(4));

        GenericRecord record = recordMap.get("1");
        Assert.assertEquals(record.get("AccountId").toString(), "1");
        Assert.assertEquals(record.get("ProductId").toString(), "1");
        Assert.assertEquals(record.get("ExtensionAttr1").toString(), "Ext1");

        record = recordMap.get("3");
        Assert.assertEquals(record.get("AccountId").toString(), "2");
        Assert.assertEquals(record.get("ProductId").toString(), "1");

        record = recordMap.get("4");
        Assert.assertEquals(record.get("AccountId").toString(), "2");
        Assert.assertEquals(record.get("ProductId").toString(), "2");

        record = recordMap.get("5");
        Assert.assertEquals(record.get("AccountId").toString(), "3");
        Assert.assertEquals(record.get("ProductId").toString(), "1");
        Assert.assertEquals(record.get("TransactionType").toString(), "PurchaseHistory");
        Assert.assertEquals(record.get("ExtensionAttr1").toString(), "Ext2");
        Assert.assertNotNull(record.get("CREATION_DATE"));
        Assert.assertNotNull(record.get("UPDATE_DATE"));

    }

    private void verifyRegisteredTable(String tableName, int attrs) {
        Table table = metadataProxy.getTable(customerSpace.toString(), tableName);
        Assert.assertNotNull(table);
        List<Attribute> attributes = table.getAttributes();
        Assert.assertEquals(new Integer(attributes.size()), new Integer(attrs));
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        Integer rowCount = 0;
        Map<String, List<GenericRecord>> recordMap = new HashMap<>();
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String id = record.get("AccountId").toString();
            if (!recordMap.containsKey(id))
                recordMap.put(id, new ArrayList<>());
            recordMap.get(id).add(record);
            rowCount++;
        }
        Assert.assertEquals(rowCount, new Integer(6));
        assertAccount1(recordMap);
        assertAccount2(recordMap);
        assertAccount3(recordMap);
    }

    private void assertAccount1(Map<String, List<GenericRecord>> recordMap) {
        List<GenericRecord> subRecords = recordMap.get("1");
        Assert.assertEquals(subRecords.size(), 1);
        GenericRecord genericRecord = subRecords.get(0);
        Assert.assertEquals(genericRecord.get("AccountId").toString(), "1");
        Assert.assertEquals(genericRecord.get("ProductId").toString(), "1");
    }

    private void assertAccount2(Map<String, List<GenericRecord>> recordMap) {
        List<GenericRecord> subRecords;
        subRecords = recordMap.get("2");
        Assert.assertEquals(subRecords.size(), 3);
        Assert.assertEquals(subRecords.get(0).get("AccountId").toString(), "2");
        Assert.assertEquals(subRecords.get(1).get("AccountId").toString(), "2");
        Assert.assertEquals(subRecords.get(2).get("AccountId").toString(), "2");
    }

    private void assertAccount3(Map<String, List<GenericRecord>> recordMap) {
        List<GenericRecord> subRecords;
        subRecords = recordMap.get("3");
        Assert.assertEquals(subRecords.size(), 2);
        GenericRecord diskRecord = null;
        GenericRecord keyboardRecord = null;
        for (GenericRecord record : subRecords) {
            if (record.get("ProductId").toString().equals("1")) {
                diskRecord = record;
            } else {
                keyboardRecord = record;
            }
        }
        Assert.assertEquals(diskRecord.get("AccountId").toString(), "3");
        Assert.assertEquals(diskRecord.get("TransactionType").toString(), "PurchaseHistory");
        Assert.assertEquals(diskRecord.get("TransactionDate").toString(), "2017-08-17");
        Assert.assertEquals(diskRecord.get("ProductId").toString(), "1");
        Assert.assertEquals(diskRecord.get("TotalAmount").toString(), "30.0");
        Assert.assertEquals(diskRecord.get("TotalQuantity").toString(), "3");

        Assert.assertEquals(keyboardRecord.get("AccountId").toString(), "3");
        Assert.assertEquals(keyboardRecord.get("TransactionType").toString(), "PurchaseHistory");
        Assert.assertEquals(keyboardRecord.get("TransactionDate").toString(), "2017-08-17");
        Assert.assertEquals(keyboardRecord.get("ProductId").toString(), "3");
        Assert.assertEquals(keyboardRecord.get("TotalAmount").toString(), "10.0");
        Assert.assertEquals(keyboardRecord.get("TotalQuantity").toString(), "1");
    }

    @Test(groups = "deployment", enabled = false)
    public void createData() {
        uploadAccountTable();
        uploadTable1();
        uploadTable2();
        uploadTable3();
    }

    private void initProductTable() {
        productTable.put("1", "product1");
        productTable.put("2", "product2");
        productTable.put("3", "product3");
        productTable.put("4", "product4");
    }

    private void uploadAccountTable() {
        Object[][] data = {{ "1" }, { "2" }, { "3" }, { "4" }, { "5" }};

        uploadDataToHdfs(data, accountFieldNames, accountClz,
                "/" + "PipelineConsolidateTrxDeploymentTestNG" + "/" + accountTableName + ".avro", accountTableName);
    }

    private void uploadTable1() {
        Object[][] data = {
                { "1", "1", null, "PurchaseHistory", "1" /* " Disk" */, 10D, 1L, "Order1", 1502755200000L, "Ext1" }, //
                { "3", "2", null, "PurchaseHistory", "1", 10D, 1L, "Order1", 1502755200000L, "Ext1" }, //
                { "4", "2", null, "PurchaseHistory", "2" /* Monitor */, 10D, 1L, "Order1", 1502755200000L, "Ext1" }, //
        };

        uploadDataToHdfs(data, fieldNames, clz,
                "/" + "PipelineConsolidateTrxDeploymentTestNG" + "/" + tableName1 + ".avro", tableName1);
    }

    private void uploadTable2() {
        Object[][] data = { { "4", "2", null, "PurchaseHistory", "2", 10D, 1L, "Order1", 1502755200000L, "Ext2" }, //
                { "5", "3", null, "PurchaseHistory", "1", 10D, 1L, "Order1", 1503001576000L, "Ext2" }, //
        };
        uploadDataToHdfs(data, fieldNames, clz,
                "/" + "PipelineConsolidateTrxDeploymentTestNG" + "/" + tableName2 + ".avro", tableName2);
    }

    private void uploadTable3() {
        Object[][] data = { { "4", "2", null, "PurchaseHistory", "2", 10D, 1L, "Order1", 1503001576000L, "Ext3" }, //
                { "5", "3", null, "PurchaseHistory", "1", 10D, 1L, "Order1", 1503001577000L, "Ext3" }, //
                { "6", "3", null, "PurchaseHistory", "1", 10D, 1L, "Order1", 1503001578000L, "Ext3" }, //
                { "7", "3", null, "PurchaseHistory", "3" /* Keyboard */, 10D, 1L, "Order1", 1503001578000L, "Ext3" }, //
        };
        uploadDataToHdfs(data, fieldNames, clz,
                "/" + "PipelineConsolidateTrxDeploymentTestNG" + "/" + tableName3 + ".avro", tableName3);
    }

}
