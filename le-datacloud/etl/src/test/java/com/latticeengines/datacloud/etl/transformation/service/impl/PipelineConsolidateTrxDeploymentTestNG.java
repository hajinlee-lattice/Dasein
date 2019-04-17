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

import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.dataflow.transformation.ConsolidateDataFlow;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodCollectorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodConvertorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodDataAggregaterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodDataCleanerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodDataDistributorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodDataFilterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodDateConvertorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ProductMapperConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransactionStandardizerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.transaction.ProductStatus;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.util.TimeSeriesUtils;

public class PipelineConsolidateTrxDeploymentTestNG extends PipelineTransformationDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PipelineConsolidateTrxDeploymentTestNG.class);

    @Autowired
    private YarnConfiguration yarnConfiguration;

    private String tableName1 = "ConsolidateTrxTable1";
    private String tableName2 = "ConsolidateTrxTable2";
    private String tableName3 = "ConsolidateTrxTable3";
    private String accountTableName = "AccountTable1";
    private String productTableName = "ProductTable1";

    private static final String SORTED_TABLE_PREFIX = "SortedDailyTransaction";
    private static final String SORTED_PERIOD_TABLE_PREFIX = "SortedPeriodTransaction";
    private static final String AGGREGATED_PERIOD_TABLE_PREFIX = "AggregatedPeriodTransaction";

    private List<String> fieldNames = Arrays.asList("TransactionId", "AccountId", "ContactId", "TransactionType", "ProductId",
            "Amount", "Quantity", "OrderId", "TransactionTime", "ExtensionAttr1");
    private List<Class<?>> clz = Arrays.asList((Class<?>) String.class, String.class, String.class, String.class, String.class,
            Double.class, Long.class, String.class, Long.class, String.class);

    private List<String> accountFieldNames = Collections.singletonList("AccountId");
    private List<Class<?>> accountClz = Collections.singletonList((Class<?>) String.class);

    private static final CustomerSpace customerSpace = CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE);
    List<TransformationStepConfig> steps = new ArrayList<>();

    private Table rawTable;
    private Table dailyTable;
    private Table periodTable;
    private HashMap<String, List<Product>> productMap = new HashMap<>();

    private int inputMergeStep;
    private int dailyStep;
    private int standardizeStep;
    private int dayPeriodStep;
    private int dailyRawStep;
    private int productAgrStep;
    private int dailyAgrStep;
    private int periodedStep;
    private int periodDataStep;
    private int periodAgrStep;
    private int periodsStep;
    private PipelineTransformationConfiguration currentConfig = null;
    private TableSource targetTableSource = null;

    @BeforeMethod(groups = "deployment")
    public void beforeMethod() {
        prepareCleanPod("PipelineConsolidateDeploymentTestNG");
    }

    @AfterMethod(groups = "deployment")
    public void afterMethod() {

        cleanupProgressTables();

        // cleanup intermediate table
        cleanupRegisteredTable(accountTableName);
        cleanupRegisteredTable(productTableName);
        cleanupRegisteredTable(tableName1);
        cleanupRegisteredTable(tableName2);
        cleanupRegisteredTable(tableName3);

        prepareCleanPod("PipelineConsolidateDeploymentTestNG");
    }

    @Test(groups = "deployment", enabled = true)
    public void testTableToTable() {
        targetVersion = HdfsPathBuilder.dateFormat.format(new Date());

        uploadAndRegisterAccountTable();
        uploadAndRegisterProductTable();
        uploadAndRegisterTable1();
        uploadAndRegisterTable2();
        uploadAndRegisterTable3();
        initializeTsStores();

        currentConfig = getTransformationConfig();

        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);

        verifyMergedTable();
        // verifyHistoryTable();
        // confirmResultFile(progress);

    }

    @Override
    protected String getTargetSourceName() {
        return SORTED_PERIOD_TABLE_PREFIX;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return null;
    }

    @Override
    protected TableSource getTargetTableSource() {
        targetTableSource = convertTargetTableSource(SORTED_PERIOD_TABLE_PREFIX);
        return targetTableSource;
    }

    private TableSource convertTargetTableSource(String tableName) {
        return hdfsSourceEntityMgr.materializeTableSource((tableName + "_" + targetVersion), customerSpace);
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        return currentConfig;
    }

    private Boolean rebuild;
    private PipelineTransformationConfiguration getTransformationConfig() {

        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("ConsolidatePipeline");
            configuration.setVersion(targetVersion);
            configuration.setKeepTemp(true);

            String earliestDate = DateTimeUtils.toDateOnlyFromMillis("1502755200000");
            rebuild = Boolean.TRUE;
            createConsolidateSteps(steps, tableName1, tableName2, rebuild, earliestDate);
            createProfileSteps(steps, rebuild, earliestDate);
            rebuild = Boolean.FALSE;
            createConsolidateSteps(steps, tableName1, tableName2, rebuild, earliestDate);
            createProfileSteps(steps, rebuild, earliestDate);

            configuration.setSteps(steps);

            return configuration;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void createConsolidateSteps(List<TransformationStepConfig> steps, String inputTable1, String inputTable2,
            Boolean rebuild, String earliestDate) {
        int startStep = steps.size();

        log.info("Consolidate start step " + startStep);
        inputMergeStep = startStep + 0;
        dailyStep = startStep + 1;
        standardizeStep = startStep + 2;
        dayPeriodStep = startStep + 3;
        dailyRawStep = startStep + 5;
        productAgrStep = startStep + 6;
        periodedStep = startStep + 7;
        dailyAgrStep = startStep + 8;
        periodsStep = startStep + 11;
        periodDataStep = startStep + 12;
        periodAgrStep = startStep + 13;
        try {

            TransformationStepConfig inputMerge = mergeInputs(inputTable1, inputTable2);
            TransformationStepConfig daily = addTrxDate();
            TransformationStepConfig standardize = standardizeTrx(dailyStep);
            TransformationStepConfig dayPeriods = collectDays(standardizeStep);
            TransformationStepConfig dailyPartition = partitionDaily();

            steps.add(inputMerge);
            steps.add(daily);
            steps.add(standardize);
            steps.add(dayPeriods);
            steps.add(dailyPartition);
            if (!rebuild) {
                TransformationStepConfig dailyRaw = collectDailyData(earliestDate);
                TransformationStepConfig productAgr = rollupProduct();
                TransformationStepConfig perioded = addPeriod();
                TransformationStepConfig dailyAgr = aggregateDaily();
                TransformationStepConfig cleanDaily = cleanupDailyHistory();
                TransformationStepConfig updateDaily = updateDailyStore();
                TransformationStepConfig periods = collectPeriods();
                TransformationStepConfig periodData = collectPeriodData(earliestDate);
                TransformationStepConfig periodAgr = aggregatePeriods(periodDataStep);
                TransformationStepConfig cleanPeriod = cleanupPeriodHistory();
                TransformationStepConfig updatePeriod = updatePeriodStore();

                steps.add(dailyRaw);
                steps.add(productAgr);
                steps.add(perioded);
                steps.add(dailyAgr);
                steps.add(cleanDaily);
                steps.add(updateDaily);
                steps.add(periods);
                steps.add(periodData);
                steps.add(periodAgr);
                steps.add(cleanPeriod);
                steps.add(updatePeriod);

            }
        } catch (Exception e) {
            log.error("Failed to run consolidate data pipeline!", e);
            throw new RuntimeException(e);
        }
    }

    private void createProfileSteps(List<TransformationStepConfig> steps, Boolean rebuild, String earliestDate) {
        int startIdx = steps.size();
        log.info("Profile start step " + startIdx);
        try {
            if (rebuild) {
                productAgrStep = startIdx + 0;
                periodedStep = startIdx + 1;
                dailyAgrStep = startIdx + 2;
                dayPeriodStep = startIdx + 3;
                periodAgrStep = startIdx + 5;
                periodsStep = startIdx + 6;
                TransformationStepConfig productAgr = rollupProductRawTable();
                TransformationStepConfig perioded = addPeriod();
                TransformationStepConfig dailyAgr = aggregateDaily();
                TransformationStepConfig dayPeriods = collectDays(dailyAgrStep);
                TransformationStepConfig updateDaily = updateDailyStore();
                TransformationStepConfig periodAgr = aggregatePeriods(dailyAgrStep);
                TransformationStepConfig periods = collectPeriods();
                TransformationStepConfig updatePeriod = updatePeriodStore();
                steps.add(productAgr);
                steps.add(perioded);
                steps.add(dailyAgr);
                steps.add(dayPeriods);
                steps.add(updateDaily);
                steps.add(periodAgr);
                steps.add(periods);
                steps.add(updatePeriod);

                startIdx = steps.size() + 1;
            }

            TransformationStepConfig sortDaily = sort(customerSpace, dailyTable, SORTED_TABLE_PREFIX);
            TransformationStepConfig sortPeriod = sort(customerSpace, periodTable, SORTED_PERIOD_TABLE_PREFIX);
            /*
             * TransformationStepConfig aggregate = aggregate(customerSpace,
             * MASTER_TABLE_PREFIX, dailyTable.getName(), //
             * accountTable.getName(), productMap);
             */
            steps.add(sortDaily);
            steps.add(sortPeriod);
            // steps.add(aggregate);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private TransformationStepConfig mergeInputs(String tableName1, String tableName2) {

        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = Arrays.asList(tableName2, tableName1);
        step1.setBaseSources(baseSources);

        SourceTable sourceTable1 = new SourceTable(tableName1, customerSpace);
        SourceTable sourceTable2 = new SourceTable(tableName2, customerSpace);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableName1, sourceTable1);
        baseTables.put(tableName2, sourceTable2);
        step1.setBaseTables(baseTables);
        step1.setTransformer(ConsolidateDataFlow.TRANSFORMER_NAME);

        step1.setConfiguration(getConsolidateDataConfig());

        return step1;
    }

    private String getConsolidateDataConfig() {
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setSrcIdField(TableRoleInCollection.AggregatedTransaction.getPrimaryKey().name());
        config.setMasterIdField(TableRoleInCollection.AggregatedTransaction.getPrimaryKey().name());
        config.setCreateTimestampColumn(true);
        config.setColumnsFromRight(new HashSet<>(Collections.singletonList("CREATION_DATE")));
        config.setCompositeKeys(
                Arrays.asList("AccountId", "ContactId", "ProductId", "TransactionType", "TransactionTime"));
        return JsonUtils.serialize(config);
    }

    private TransformationStepConfig addTrxDate() {
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PERIOD_DATE_CONVERTOR);
        step2.setInputSteps(Collections.singletonList(inputMergeStep));
        PeriodDateConvertorConfig config = new PeriodDateConvertorConfig();
        config.setTrxTimeField(InterfaceName.TransactionTime.name());
        config.setTrxDateField(InterfaceName.TransactionDate.name());
        config.setTrxDayPeriodField(InterfaceName.TransactionDayPeriod.name());
        step2.setConfiguration(JsonUtils.serialize(config));
        return step2;
    }

    private TransformationStepConfig standardizeTrx(int inputStep) {
        List<String> stringFields = new ArrayList<String>();
        List<String> longFields = new ArrayList<String>();
        List<String> intFields = new ArrayList<String>();
        Table rawTemplate = SchemaRepository.instance().getSchema(SchemaInterpretation.TransactionRaw, true);
        getTableFields(rawTemplate, stringFields, longFields, intFields);

        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.TRANSACTION_STANDARDIZER);
        step.setInputSteps(Collections.singletonList(inputStep));
        TransactionStandardizerConfig config = new TransactionStandardizerConfig();
        config.setStringFields(stringFields);
        config.setLongFields(longFields);
        config.setIntFields(intFields);
        config.setCustomField(InterfaceName.CustomTrxField.name());
        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private void getTableFields(Table table, List<String> stringFields, List<String> longFields, List<String> intFields) {
        List<Attribute> attrs = table.getAttributes();
        for (Attribute attr: attrs) {
            Type attrType = Type.valueOf(attr.getPhysicalDataType());
            String attrName = attr.getName();
            if (attrType == Type.STRING) {
                stringFields.add(attrName);
            } else if (attrType == Type.LONG) {
                longFields.add(attrName);
            } else if (attrType == Type.INT) {
                intFields.add(attrName);
            } else {
                log.warn("Invalid attribute type for " + attrName);
            }
        }
        Collections.sort(stringFields);
        Collections.sort(longFields);
        Collections.sort(intFields);
    }

    private TransformationStepConfig collectDays(int inputStep) {
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PERIOD_COLLECTOR);
        step2.setInputSteps(Collections.singletonList(inputStep));
        PeriodCollectorConfig config = new PeriodCollectorConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());
        step2.setConfiguration(JsonUtils.serialize(config));
        return step2;
    }

    private TransformationStepConfig partitionDaily() {

        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PERIOD_DATA_DISTRIBUTOR);
        List<Integer> inputSteps = new ArrayList<>();
        inputSteps.add(dayPeriodStep);
        inputSteps.add(standardizeStep);
        step2.setInputSteps(inputSteps);

        String tableSourceName = "CustomerUniverse";
        String sourceTableName = rawTable.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step2.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step2.setBaseTables(baseTables);

        PeriodDataDistributorConfig config = new PeriodDataDistributorConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());
        step2.setConfiguration(JsonUtils.serialize(config));
        return step2;
    }

    private TransformationStepConfig collectDailyData(String earliestTransaction) {
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PERIOD_DATA_FILTER);
        step2.setInputSteps(Collections.singletonList(dayPeriodStep));

        String tableSourceName = "CustomerUniverse";
        String sourceTableName = rawTable.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step2.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step2.setBaseTables(baseTables);

        PeriodDataFilterConfig config = new PeriodDataFilterConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());
        config.setEarliestTransactionDate(earliestTransaction);
        step2.setConfiguration(JsonUtils.serialize(config));
        return step2;
    }

    private TransformationStepConfig rollupProduct() {
        TransformationStepConfig stepConfig = new TransformationStepConfig();
        stepConfig.setTransformer(DataCloudConstants.PRODUCT_MAPPER);
        stepConfig.setInputSteps(Collections.singletonList(dailyRawStep));
        ProductMapperConfig config = new ProductMapperConfig();
        config.setProductField(InterfaceName.ProductId.name());
        config.setProductTypeField(InterfaceName.ProductType.name());

        List<String> baseSources = Arrays.asList(productTableName);
        stepConfig.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(productTableName, new SourceTable(productTableName, customerSpace));
        stepConfig.setBaseTables(baseTables);

        stepConfig.setConfiguration(JsonUtils.serialize(config));
        return stepConfig;
    }

    private TransformationStepConfig rollupProductRawTable() {
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PRODUCT_MAPPER);
        String tableSourceName = "CustomerUniverse";
        String sourceTableName = rawTable.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Arrays.asList(tableSourceName, productTableName);
        step2.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        baseTables.put(productTableName, new SourceTable(productTableName, customerSpace));
        step2.setBaseTables(baseTables);

        ProductMapperConfig config = new ProductMapperConfig();
        config.setProductField(InterfaceName.ProductId.name());
        config.setProductTypeField(InterfaceName.ProductType.name());

        step2.setConfiguration(JsonUtils.serialize(config));
        return step2;
    }

    private TransformationStepConfig addPeriod() {
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PERIOD_CONVERTOR);
        step2.setInputSteps(Collections.singletonList(productAgrStep));
        PeriodConvertorConfig config = new PeriodConvertorConfig();
        config.setTrxDateField(InterfaceName.TransactionDate.name());
        config.setPeriodField(InterfaceName.PeriodId.name());
        config.setPeriodStrategies(Arrays.asList(PeriodStrategy.CalendarMonth));
        step2.setConfiguration(JsonUtils.serialize(config));
        return step2;
    }

    private TransformationStepConfig cleanupDailyHistory() {
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PERIOD_DATA_CLEANER);
        step2.setInputSteps(Collections.singletonList(dayPeriodStep));

        String tableSourceName = "CustomerUniverse";
        String sourceTableName = dailyTable.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step2.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step2.setBaseTables(baseTables);
        step2.setInputSteps(Collections.singletonList(productAgrStep));
        PeriodDataCleanerConfig config = new PeriodDataCleanerConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());
        step2.setConfiguration(JsonUtils.serialize(config));
        return step2;
    }

    private TransformationStepConfig updateDailyStore() {
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PERIOD_DATA_DISTRIBUTOR);
        List<Integer> inputSteps = new ArrayList<>();
        inputSteps.add(dayPeriodStep);
        inputSteps.add(dailyAgrStep);
        step2.setInputSteps(inputSteps);

        String tableSourceName = "CustomerUniverse";
        String sourceTableName = dailyTable.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step2.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step2.setBaseTables(baseTables);

        PeriodDataDistributorConfig config = new PeriodDataDistributorConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());
        step2.setConfiguration(JsonUtils.serialize(config));
        return step2;
    }

    private TransformationStepConfig collectPeriods() {
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PERIOD_COLLECTOR);
        step2.setInputSteps(Collections.singletonList(dailyAgrStep));
        PeriodCollectorConfig config = new PeriodCollectorConfig();
        config.setPeriodField(InterfaceName.PeriodId.name());
        step2.setConfiguration(JsonUtils.serialize(config));
        return step2;
    }

    private TransformationStepConfig collectPeriodData(String earliestTransaction) {
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PERIOD_DATA_FILTER);
        step2.setInputSteps(Collections.singletonList(periodsStep));

        String tableSourceName = "CustomerUniverse";
        String sourceTableName = periodTable.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step2.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step2.setBaseTables(baseTables);

        PeriodDataFilterConfig config = new PeriodDataFilterConfig();
        config.setPeriodField(InterfaceName.PeriodId.name());
        // config.setPeriodStrategy(PeriodStrategy.CalendarMonth);
        config.setEarliestTransactionDate(earliestTransaction);
        step2.setConfiguration(JsonUtils.serialize(config));
        return step2;
    }

    private TransformationStepConfig aggregatePeriods(int inputStep) {
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PERIOD_DATA_AGGREGATER);
        step2.setInputSteps(Collections.singletonList(inputStep));
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(AGGREGATED_PERIOD_TABLE_PREFIX);
        targetTable.setExpandBucketedAttrs(false);
        step2.setTargetTable(targetTable);
        PeriodDataAggregaterConfig config = new PeriodDataAggregaterConfig();
        config.setSumFields(Collections.singletonList("TotalAmount"));
        config.setSumOutputFields(Collections.singletonList("TotalAmount"));
        config.setSumLongFields(Collections.singletonList("TotalQuantity"));
        config.setSumLongOutputFields(Collections.singletonList("TotalQuantity"));
        config.setGroupByFields(Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.ContactId.name(),
                InterfaceName.ProductId.name(), InterfaceName.TransactionType.name(), InterfaceName.PeriodId.name()));
        // config.setPeriodStrategy(PeriodStrategy.CalendarMonth);
        step2.setConfiguration(JsonUtils.serialize(config));
        return step2;
    }

    private TransformationStepConfig cleanupPeriodHistory() {
        log.info("Clean up period history from " + periodsStep);
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PERIOD_DATA_CLEANER);
        step2.setInputSteps(Collections.singletonList(periodsStep));

        String tableSourceName = "CustomerUniverse";
        String sourceTableName = periodTable.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step2.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step2.setBaseTables(baseTables);

        PeriodDataCleanerConfig config = new PeriodDataCleanerConfig();
        config.setPeriodField(InterfaceName.PeriodId.name());
        step2.setConfiguration(JsonUtils.serialize(config));
        return step2;
    }

    private TransformationStepConfig updatePeriodStore() {
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PERIOD_DATA_DISTRIBUTOR);
        List<Integer> inputSteps = new ArrayList<>();
        inputSteps.add(periodsStep);
        inputSteps.add(periodAgrStep);
        step2.setInputSteps(inputSteps);

        String tableSourceName = "CustomerUniverse";
        String sourceTableName = periodTable.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step2.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step2.setBaseTables(baseTables);

        PeriodDataDistributorConfig config = new PeriodDataDistributorConfig();
        config.setPeriodField(InterfaceName.PeriodId.name());
        step2.setConfiguration(JsonUtils.serialize(config));
        return step2;
    }

    private TransformationStepConfig aggregateDaily() {
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PERIOD_DATA_AGGREGATER);
        step2.setInputSteps(Collections.singletonList(periodedStep));
        PeriodDataAggregaterConfig config = new PeriodDataAggregaterConfig();
        config.setSumFields(Collections.singletonList("Amount"));
        config.setSumOutputFields(Collections.singletonList("TotalAmount"));
        config.setSumLongFields(Collections.singletonList("Quantity"));
        config.setSumLongOutputFields(Collections.singletonList("TotalQuantity"));
        config.setGroupByFields(Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.ContactId.name(),
                InterfaceName.ProductId.name(), InterfaceName.TransactionType.name(),
                InterfaceName.TransactionDate.name(), InterfaceName.PeriodId.name(),
                InterfaceName.TransactionDayPeriod.name()));
        // config.setPeriodStrategy(PeriodStrategy.CalendarMonth);
        step2.setConfiguration(JsonUtils.serialize(config));
        return step2;
    }

    private TransformationStepConfig sort(CustomerSpace customerSpace, Table table, String prefix) {
        TransformationStepConfig step = new TransformationStepConfig();
        String tableSourceName = "CustomerUniverse";
        SourceTable sourceTable = new SourceTable(table.getName(), customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);
        step.setTransformer(DataCloudConstants.TRANSFORMER_SORTER);

        SorterConfig config = new SorterConfig();
        config.setPartitions(20);
        String sortingKey = TableRoleInCollection.SortedProduct.getForeignKeysAsStringList().get(0);
        config.setSortingField(sortingKey);
        config.setCompressResult(true);
        step.setConfiguration(JsonUtils.serialize(config));

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(prefix);
        step.setTargetTable(targetTable);

        return step;
    }

    private void cleanupRegisteredTable(String tableName) {
        metadataProxy.deleteTable(customerSpace.toString(), tableName);
    }

    private void verifyMergedTable() {
        verifyRegisteredTable(rawTable, fieldNames.size() + 3);
        verifyRegisteredTable(dailyTable, fieldNames.size() + 3);
        verifyRegisteredTable(periodTable, fieldNames.size() + 3);
        // verifyRawRecords(rawTable);
        verifyAggregatedRecords(dailyTable);
        verifyAggregatedRecords(periodTable);
    }

    // private void verifyHistoryTable() {
    // String historyTableFullName =
    // TableSource.getFullTableName(historyTableName1, targetVersion);
    // verifyRegisteredTable(historyTableFullName, productMap.size() * 3 + 1);
    // verifyRecordsInHistoryTable(historyTableFullName);
    // }

    @SuppressWarnings("unused")
    private void verifyRawRecords(Table table) {
        log.info("Start to verify records one by one.");
        List<GenericRecord> records = getRecordFromTable(table);
        Integer rowCount = 0;
        Map<String, GenericRecord> recordMap = new HashMap<>();
        for (GenericRecord record : records) {
            String id = String.valueOf(record.get(InterfaceName.TransactionId.name()));
            recordMap.put(id, record);
            rowCount++;
        }
        Assert.assertEquals(rowCount, new Integer(8));

        GenericRecord record = recordMap.get("1");
        Assert.assertEquals(record.get("AccountId").toString(), "1");
        Assert.assertEquals(record.get("ProductId").toString(), "1");

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

    }

    private void verifyAggregatedRecords(Table table) {
        log.info("Start to verify records one by one.");
        List<GenericRecord> records = getRecordFromTable(table);
        Integer rowCount = 0;
        Map<String, GenericRecord> recordMap = new HashMap<>();
        for (GenericRecord record : records) {
            String accountId = String.valueOf(record.get(InterfaceName.AccountId.name()));
            String productId = String.valueOf(record.get(InterfaceName.ProductId.name()));
            recordMap.put((accountId + "-" + productId), record);
            rowCount++;
        }
        Assert.assertEquals(rowCount, new Integer(9));

        GenericRecord record = recordMap.get("1-b1");
        Assert.assertEquals(record.get("TotalAmount"), 20D);
        Assert.assertEquals(record.get("TotalQuantity"), 2L);

        record = recordMap.get("2-b2");
        Assert.assertEquals(record.get("TotalAmount"), 20D);
        Assert.assertEquals(record.get("TotalQuantity"), 2L);

        record = recordMap.get("2-b1");
        Assert.assertEquals(record.get("TotalAmount"), 20D);
        Assert.assertEquals(record.get("TotalQuantity"), 2L);

        record = recordMap.get("3-b1");
        Assert.assertEquals(record.get("TotalAmount"), 20D);
        Assert.assertEquals(record.get("TotalQuantity"), 2L);
    }

    private void verifyRegisteredTable(Table table, int attrs) {
        Assert.assertNotNull(table);
        // List<Attribute> attributes = table.getAttributes();
        // Assert.assertEquals(new Integer(attributes.size()), new
        // Integer(attrs));
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
        Assert.assertEquals(rowCount, new Integer(4));
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

    private void initializeTsStores() {
        rawTable = buildPeriodStore(SchemaInterpretation.TransactionRaw);
        dailyTable = buildPeriodStore(SchemaInterpretation.TransactionDailyAggregation);
        periodTable = buildPeriodStore(SchemaInterpretation.TransactionPeriodAggregation);
    }

    private Table buildPeriodStore(SchemaInterpretation schema) {
        Table table = SchemaRepository.instance().getSchema(schema);

        String hdfsPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace, "").toString()
                + "/" + schema;
        try {
            log.info("Initialize period store " + hdfsPath);
            if (HdfsUtils.fileExists(yarnConfiguration, hdfsPath)) {
                TimeSeriesUtils.cleanupPeriodData(yarnConfiguration, hdfsPath);
                log.info("Removing file from " + hdfsPath + "/*.avro");
            } else {
                HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
                log.info("Creating extract path " + hdfsPath);
            }
        } catch (Exception e) {
            log.error("Failed to initialize period store " + hdfsPath);
        }

        Extract extract = new Extract();
        extract.setName("extract_target");
        extract.setPath(hdfsPath + "/");
        extract.setExtractionTimestamp(DateTime.now().getMillis());
        extract.setProcessedRecords(1L);

        table.setExtracts(Collections.singletonList(extract));
        metadataProxy.updateTable(customerSpace.toString(), table.getName(), table);
        return table;
    }

    protected List<GenericRecord> getRecordFromTable(Table table) {
        String tableDir = table.getExtracts().get(0).getPath();
        return AvroUtils.getDataFromGlob(yarnConfiguration, tableDir + "/*.avro");
    }

    private void uploadAndRegisterAccountTable() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        for (int i = 0; i < accountFieldNames.size(); i++) {
            columns.add(Pair.of(accountFieldNames.get(i), accountClz.get(i)));
        }
        Object[][] data = { { "1" }, { "2" }, { "3" }, { "4" }, { "5" } };
        uploadAndRegisterTableSource(columns, data, accountTableName);
    }

    private List<String> productFields = Arrays.asList(InterfaceName.ProductId.name(), InterfaceName.ProductName.name(),
            InterfaceName.ProductBundle.name(), InterfaceName.ProductLine.name(), InterfaceName.ProductFamily.name(),
            InterfaceName.ProductCategory.name(), InterfaceName.ProductStatus.name());

    private void uploadAndRegisterProductTable() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        for (int i = 0; i < productFields.size(); i++) {
            columns.add(Pair.of(productFields.get(i), String.class));
        }
        Object[][] data = { 
                { "1", "sku1", "b1", null, null, null, ProductStatus.Active.name() }, //
                { "1", "sku1", "b2", null, null, null, ProductStatus.Active.name() }, //
                { "1", "sku1", null, "pl1", "pf1", "pc1", ProductStatus.Active.name() }, //
                { "3", "sku3", "b1", "pl1", "pf1", "pc1", ProductStatus.Active.name() }, //
                { "4", "sku4", null, null, null, null, ProductStatus.Active.name() }, //
                { "5", "sku5", "b3", "pl1", "pf1", "pc1", ProductStatus.Active.name() } //
        }; //
        uploadAndRegisterTableSource(columns, data, productTableName);
    }
    
    private void uploadAndRegisterTable1() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            columns.add(Pair.of(fieldNames.get(i), clz.get(i)));
        }
        Object[][] data = {
// "TransactionId", "AccountId", "ContactId", "TransactionType", "ProductId", "Amount", "Quantity", "OrderId", "TransactionTime", "ExtensionAttr1"
                { "1", "1", null, "PurchaseHistory", "1" /* " Disk" */, 10D, 1L, "Order1", 1502755200000L, "Ext1" }, //
                { "3", "2", null, "PurchaseHistory", "1", 10D, 1L, "Order1", 1502755200000L, "Ext1" }, //
                { "4", "2", null, "PurchaseHistory", "2" /* Monitor */, 10D, 1L, "Order1", 1502755200000L, "Ext1" }, //
        };
        uploadAndRegisterTableSource(columns, data, tableName1);
    }

    private void uploadAndRegisterTable2() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            columns.add(Pair.of(fieldNames.get(i), clz.get(i)));
        }
        Object[][] data = {
                { "4", "2", null, "PurchaseHistory", "2", 10D, 1L, "Order1", 1502755200000L, "Ext2" }, //
                { "5", "3", null, "PurchaseHistory", "1", 10D, 1L, "Order1", 1503001576000L, "Ext2" }, //
        };
        uploadAndRegisterTableSource(columns, data, tableName2);
    }

    private void uploadAndRegisterTable3() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            columns.add(Pair.of(fieldNames.get(i), clz.get(i)));
        }
        Object[][] data = {
                { "4", "2", null, "PurchaseHistory", "2", 10D, 1L, "Order1", 1503001576000L, "Ext3" }, //
                { "5", "3", null, "PurchaseHistory", "1", 10D, 1L, "Order1", 1503001577000L, "Ext3" }, //
                { "6", "3", null, "PurchaseHistory", "1", 10D, 1L, "Order1", 1503001578000L, "Ext3" }, //
                { "7", "3", null, "PurchaseHistory", "3" /* Keyboard */, 10D, 1L, "Order1", 1503001578000L, "Ext3" }, //
        };
        uploadAndRegisterTableSource(columns, data, tableName3);
    }

}
