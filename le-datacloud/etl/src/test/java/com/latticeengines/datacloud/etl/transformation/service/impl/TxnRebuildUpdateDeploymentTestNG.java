package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
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
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransactionStandardizerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.metadata.transaction.ProductStatus;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.PeriodStrategyUtils;
import com.latticeengines.domain.exposed.util.TransactionUtils;

/**
 * Test with NatualCalendar
 *
 */
public class TxnRebuildUpdateDeploymentTestNG extends PipelineTransformationDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(TxnRebuildUpdateDeploymentTestNG.class);

    @SuppressWarnings("unused")
    private static final String SORTED_DAILY_PREFIX = TableRoleInCollection.AggregatedTransaction.name();
    @SuppressWarnings("unused")
    private static final String SORTED_PERIOD_TABLE_PREFIX = TableRoleInCollection.AggregatedPeriodTransaction.name();

    private static final CustomerSpace customerSpace = CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE);

    private GeneralSource source = new GeneralSource(TxnRebuildUpdateDeploymentTestNG.class.getName());

    private String actTable = "ActTable";
    private String productTable = "ProductTable";
    private String rawTxnTable1 = "RawTxnTable1"; // Rebuild
    private String rawTxnTable2 = "RawTxnTable2"; // Rebuild
    private String rawTxnTable3 = "RawTxnTable3"; // Update

    private Table rawTxnStore;
    private Table dailyTxnStore;
    private List<Table> periodTxnStore;

    private List<PeriodStrategy> periodStrategies = PeriodStrategy.NATURAL_PERIODS;

    private int previousStep;
    private int dailyAgrStep;

    private static final String MIN_TXN_DATE = "2017-01-01";

    @Test(groups = "deployment", enabled = true)
    public void testTransformation() {
        prepareActTable();
        prepareProductTable();
        prepareRawTxnTablesForRebuild();
        prepareRawTxnTablesForUpdate();
        initializeTsStores();

        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        // confirmIntermediateSource(depivotedMetrics, null);
        confirmResultFile(progress);
    }

    @AfterClass(groups = "deployment", enabled = true)
    public void afterMethod() {
        cleanupProgressTables();
        cleanupRegisteredTable(actTable);
        cleanupRegisteredTable(rawTxnTable1);
        cleanupRegisteredTable(rawTxnTable2);
        cleanupRegisteredTable(rawTxnTable3);
        cleanupRegisteredTable(rawTxnStore.getName());
        cleanupRegisteredTable(dailyTxnStore.getName());
        for (Table periodTable : periodTxnStore) {
            cleanupRegisteredTable(periodTable.getName());
        }
    }

    /**********************************
     * Pipeline Configuration
     **********************************/

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("TransactionRebuildUpdate");
        configuration.setVersion(targetVersion);
        configuration.setKeepTemp(true);

        List<TransformationStepConfig> steps = new ArrayList<>();
        previousStep = -1;

        // Rebuild
        steps.addAll(mergeTxnRebuild(Arrays.asList(rawTxnTable1, rawTxnTable2), false));
        steps.addAll(aggregateDailyTxnRebuild());
        steps.addAll(aggregatePeriodTxnRebuild());

        // Update
        steps.addAll(aggregatePeriodTxnUpdate());

        steps.get(steps.size() - 1).setTargetSource(source.getSourceName());
        configuration.setSteps(steps);
        return configuration;
    }

    // ---------------------- Merge Transaction ---------------------- //
    private List<TransformationStepConfig> mergeTxnRebuild(List<String> rawTxnTables, boolean update) {
        int txnDateStep, standardizeStep, collectDaysStep;
        List<TransformationStepConfig> steps = new ArrayList<>();
        // Don't use target table
        steps.add(mergeInputs(rawTxnTables, false, false, true));
        steps.add(addTrxDate(previousStep));
        txnDateStep = previousStep;
        if (update) {
            steps.add(mergeRaw(txnDateStep));
        }
        steps.add(standardizeTrx(txnDateStep));
        standardizeStep = previousStep;
        steps.add(collectDays(standardizeStep));
        collectDaysStep = previousStep;
        steps.add(partitionDaily(collectDaysStep, standardizeStep));

        return steps;
    }

    // Should be sync with BaseMergeImports.mergeInputs
    private TransformationStepConfig mergeInputs(List<String> inputTableNames, boolean useTargetTable,
            boolean isDedupeSource, boolean mergeOnly) {
        TransformationStepConfig step = new TransformationStepConfig();
        List<String> baseSources = inputTableNames;
        step.setBaseSources(baseSources);

        Map<String, SourceTable> baseTables = new HashMap<>();
        for (String inputTableName : inputTableNames) {
            baseTables.put(inputTableName, new SourceTable(inputTableName, customerSpace));
        }
        step.setBaseTables(baseTables);
        step.setTransformer(DataCloudConstants.TRANSFORMER_CONSOLIDATE_DATA);
        step.setConfiguration(getConsolidateDataConfig(isDedupeSource, true, mergeOnly, false));
        if (useTargetTable) {
            TargetTable targetTable = new TargetTable();
            targetTable.setCustomerSpace(customerSpace);
            targetTable.setNamePrefix(BusinessEntity.Transaction.getBatchStore().name() + "_Merged");
            step.setTargetTable(targetTable);
        }
        previousStep++;
        return step;
    }

    // Should be sync with BaseMergeImports.getConsolidateDataConfig
    private String getConsolidateDataConfig(boolean isDedupeSource, boolean addTimettamps, boolean isMergeOnly,
            boolean copyCreateTime) {
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setSrcIdField(InterfaceName.Id.name());
        config.setMasterIdField(BusinessEntity.Transaction.getBatchStore().getPrimaryKey().name());
        config.setDedupeSource(isDedupeSource);
        config.setMergeOnly(isMergeOnly);
        config.setAddTimestamps(addTimettamps);
        if (copyCreateTime) {
            config.setColumnsFromRight(Collections.singleton(InterfaceName.CDLCreatedTime.name()));
        }
        return JsonUtils.serialize(config);
    }

    // Should be sync with MergeTransaction.addTrxDate
    private TransformationStepConfig addTrxDate(int mergeStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATE_CONVERTOR);
        step.setInputSteps(Collections.singletonList(mergeStep));
        PeriodDateConvertorConfig config = new PeriodDateConvertorConfig();
        config.setTrxTimeField(InterfaceName.TransactionTime.name());
        config.setTrxDateField(InterfaceName.TransactionDate.name());
        config.setTrxDayPeriodField(InterfaceName.TransactionDayPeriod.name());
        step.setConfiguration(JsonUtils.serialize(config));
        previousStep++;
        return step;
    }

    // Should be sync with MergeTransaction.mergeRaw
    TransformationStepConfig mergeRaw(int txnDateStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        List<String> baseSources;
        Map<String, SourceTable> baseTables;
        String rawName = rawTxnStore.getName();
        baseSources = Collections.singletonList(rawName);
        baseTables = new HashMap<>();
        SourceTable sourceMasterTable = new SourceTable(rawName, customerSpace);
        baseTables.put(rawName, sourceMasterTable);
        step.setBaseSources(baseSources);
        step.setBaseTables(baseTables);
        step.setInputSteps(Collections.singletonList(txnDateStep));
        step.setTransformer(DataCloudConstants.TRANSFORMER_CONSOLIDATE_DATA);
        step.setConfiguration(getConsolidateDataConfig(false, false, true, false));
        previousStep++;
        return step;
    }

    // Should be sync with MergeTransaction.standardizeTrx
    private TransformationStepConfig standardizeTrx(int txnDateStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.TRANSACTION_STANDARDIZER);
        step.setInputSteps(Collections.singletonList(txnDateStep));
        TransactionStandardizerConfig config = new TransactionStandardizerConfig();
        List<String> stringFields = new ArrayList<>();
        List<String> longFields = new ArrayList<>();
        List<String> intFields = new ArrayList<>();
        Table rawTemplate = SchemaRepository.instance().getSchema(SchemaInterpretation.TransactionRaw, true);
        getTableFields(rawTemplate, stringFields, longFields, intFields);
        config.setStringFields(stringFields);
        config.setLongFields(longFields);
        config.setIntFields(intFields);
        config.setCustomField(InterfaceName.CustomTrxField.name());
        step.setConfiguration(JsonUtils.serialize(config));
        previousStep++;
        return step;
    }

    // Should be sync with MergeTransaction.getTableFields
    private void getTableFields(Table table, List<String> stringFields, List<String> longFields,
            List<String> intFields) {
        List<Attribute> attrs = table.getAttributes();
        for (Attribute attr : attrs) {
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

    // Should be sync with MergeTransaction.collectDays
    private TransformationStepConfig collectDays(int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_COLLECTOR);
        step.setInputSteps(Collections.singletonList(inputStep));
        PeriodCollectorConfig config = new PeriodCollectorConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());
        step.setConfiguration(JsonUtils.serialize(config));
        previousStep++;
        return step;
    }

    // Should be sync with MergeTransaction.collectDays
    private TransformationStepConfig partitionDaily(int collectDaysStep, int standardizeStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_DISTRIBUTOR);
        List<Integer> inputSteps = new ArrayList<>();
        inputSteps.add(collectDaysStep);
        inputSteps.add(standardizeStep);
        step.setInputSteps(inputSteps);

        String tableSourceName = "RawTransaction";
        String sourceTableName = rawTxnStore.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);

        PeriodDataDistributorConfig config = new PeriodDataDistributorConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());
        step.setConfiguration(JsonUtils.serialize(config));

        previousStep++;
        return step;
    }

    // ---------------------- Daily Transaction Aggregation
    // ---------------------- //
    private List<TransformationStepConfig> aggregateDailyTxnRebuild() {
        int dailyPeriodStep;
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(rollupProduct(productTable));
        steps.add(addPeriod(previousStep, null));
        dailyPeriodStep = previousStep;
        steps.add(aggregateDaily(previousStep));
        dailyAgrStep = previousStep;
        steps.add(collectDays(previousStep));
        steps.add(updateDailyStore(dailyPeriodStep, dailyAgrStep));
        return steps;
    }

    @SuppressWarnings("unused")
    private List<TransformationStepConfig> aggregateDailyTxnUpdate() {
        List<TransformationStepConfig> steps = new ArrayList<>();
        return steps;
        /*
         * int dailyPeriodStep; List<TransformationStepConfig> steps = new
         * ArrayList<>(); steps.add(rollupProduct(productMap));
         * steps.add(addPeriod(previousStep, null)); dailyPeriodStep =
         * previousStep; steps.add(aggregateDaily(previousStep)); dailyAgrStep =
         * previousStep; steps.add(collectDays(previousStep));
         * steps.add(updateDailyStore(dailyPeriodStep, dailyAgrStep)); return
         * steps;
         */

    }

    // Should be sync with ProfileTransaction.rollupProduct
    private TransformationStepConfig rollupProduct(String productTable) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PRODUCT_MAPPER);
        String tableSourceName = "CustomerUniverse";
        String sourceTableName = rawTxnStore.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Arrays.asList(tableSourceName, productTable);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        baseTables.put(productTable, new SourceTable(productTable, customerSpace));
        step.setBaseTables(baseTables);

        ProductMapperConfig config = new ProductMapperConfig();
        config.setProductField(InterfaceName.ProductId.name());
        config.setProductTypeField(InterfaceName.ProductType.name());

        step.setConfiguration(JsonUtils.serialize(config));
        previousStep++;
        return step;
    }

    // Should be sync with ProfileTransaction.addPeriod
    private TransformationStepConfig addPeriod(int inputStep, List<PeriodStrategy> periodStrategies) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_CONVERTOR);
        step.setInputSteps(Collections.singletonList(inputStep));
        PeriodConvertorConfig config = new PeriodConvertorConfig();
        config.setTrxDateField(InterfaceName.TransactionDate.name());
        config.setPeriodStrategies(periodStrategies);
        config.setPeriodField(InterfaceName.PeriodId.name());
        step.setConfiguration(JsonUtils.serialize(config));
        previousStep++;
        return step;
    }

    private TransformationStepConfig aggregateDaily(int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_AGGREGATER);
        step.setInputSteps(Collections.singletonList(inputStep));
        PeriodDataAggregaterConfig config = new PeriodDataAggregaterConfig();
        config.setSumFields(Arrays.asList(InterfaceName.Amount.name(), InterfaceName.Cost.name()));
        config.setSumOutputFields(Arrays.asList(InterfaceName.TotalAmount.name(), InterfaceName.TotalCost.name()));
        config.setSumLongFields(Collections.singletonList(InterfaceName.Quantity.name()));
        config.setSumLongOutputFields(Collections.singletonList(InterfaceName.TotalQuantity.name()));
        config.setCountField(Collections.singletonList(InterfaceName.TransactionTime.name()));
        config.setCountOutputField(Collections.singletonList(InterfaceName.TransactionCount.name()));
        config.setGroupByFields(Arrays.asList( //
                InterfaceName.AccountId.name(), //
                InterfaceName.ProductId.name(), //
                InterfaceName.ProductType.name(), //
                InterfaceName.TransactionType.name(), //
                InterfaceName.TransactionDate.name(), //
                InterfaceName.TransactionDayPeriod.name()));
        step.setConfiguration(JsonUtils.serialize(config));
        previousStep++;
        return step;
    }

    private TransformationStepConfig updateDailyStore(int dailyPeriodStep, int dailyAgrStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_DISTRIBUTOR);
        List<Integer> inputSteps = new ArrayList<>();
        inputSteps.add(dailyPeriodStep);
        inputSteps.add(dailyAgrStep);
        step.setInputSteps(inputSteps);

        String tableSourceName = "CustomerUniverse";
        String sourceTableName = dailyTxnStore.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);

        PeriodDataDistributorConfig config = new PeriodDataDistributorConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());
        step.setConfiguration(JsonUtils.serialize(config));
        previousStep++;
        return step;
    }

    // ---------------------- Period Transaction Aggregation
    // ---------------------- //
    private List<TransformationStepConfig> aggregatePeriodTxnRebuild() {
        int periodsStep, periodAgrStep;
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(addPeriod(dailyAgrStep, periodStrategies));
        steps.add(aggregatePeriods(previousStep));
        periodAgrStep = previousStep;
        steps.add(collectPeriods(previousStep));
        periodsStep = previousStep;
        steps.add(updatePeriodStore(periodTxnStore, periodsStep, periodAgrStep));
        return steps;
    }

    private List<TransformationStepConfig> aggregatePeriodTxnUpdate() {
        int periodsStep, periodAgrStep;
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(addPeriod(dailyAgrStep, periodStrategies));
        steps.add(collectPeriods(previousStep));
        periodsStep = previousStep;
        steps.add(collectPeriodData(previousStep));
        steps.add(addPeriod(previousStep, periodStrategies));
        steps.add(aggregatePeriods(previousStep));
        periodAgrStep = previousStep;
        steps.add(cleanupPeriodHistory(periodsStep, periodTxnStore));
        steps.add(updatePeriodStore(periodTxnStore, periodsStep, periodAgrStep));
        return steps;
    }

    private TransformationStepConfig aggregatePeriods(int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_AGGREGATER);
        step.setInputSteps(Collections.singletonList(inputStep));
        PeriodDataAggregaterConfig config = new PeriodDataAggregaterConfig();
        config.setSumFields(Arrays.asList(InterfaceName.TotalAmount.name(), InterfaceName.TotalCost.name(),
                InterfaceName.TransactionCount.name()));
        config.setSumOutputFields(Arrays.asList(InterfaceName.TotalAmount.name(), InterfaceName.TotalCost.name(),
                InterfaceName.TransactionCount.name()));
        config.setSumLongFields(Collections.singletonList(InterfaceName.TotalQuantity.name()));
        config.setSumLongOutputFields(Collections.singletonList(InterfaceName.TotalQuantity.name()));
        config.setGroupByFields(Arrays.asList( //
                InterfaceName.AccountId.name(), //
                InterfaceName.ProductId.name(), //
                InterfaceName.ProductType.name(), //
                InterfaceName.TransactionType.name(), //
                InterfaceName.PeriodId.name(), //
                InterfaceName.PeriodName.name()));
        step.setConfiguration(JsonUtils.serialize(config));
        previousStep++;
        return step;
    }

    private TransformationStepConfig collectPeriods(int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_COLLECTOR);
        step.setInputSteps(Collections.singletonList(inputStep));
        PeriodCollectorConfig config = new PeriodCollectorConfig();
        config.setPeriodField(InterfaceName.PeriodId.name());
        config.setPeriodNameField(InterfaceName.PeriodName.name());
        step.setConfiguration(JsonUtils.serialize(config));
        previousStep++;
        return step;
    }

    private TransformationStepConfig updatePeriodStore(List<Table> periodTables, int periodsStep, int periodAgrStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_DISTRIBUTOR);
        List<Integer> inputSteps = new ArrayList<>();
        inputSteps.add(periodsStep);
        inputSteps.add(periodAgrStep);
        step.setInputSteps(inputSteps);

        List<String> baseSources = new ArrayList<>();
        Map<String, SourceTable> baseTables = new HashMap<>();
        Map<String, Integer> transactionIdxes = new HashMap<>();
        for (int i = 0; i < periodTables.size(); i++) {
            Table periodTable = periodTables.get(i);
            String tableSourceName = "CustomerUniverse" + periodTable.getName();
            String sourceTableName = periodTable.getName();
            SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
            baseSources.add(tableSourceName);
            baseTables.put(tableSourceName, sourceTable);
            transactionIdxes.put(PeriodStrategyUtils.getPeriodStrategyNameFromPeriodTableName(periodTable.getName(),
                    TableRoleInCollection.ConsolidatedPeriodTransaction), i + 2);
        }
        step.setBaseSources(baseSources);
        step.setBaseTables(baseTables);

        PeriodDataDistributorConfig config = new PeriodDataDistributorConfig();
        config.setMultiPeriod(true);
        config.setPeriodField(InterfaceName.PeriodId.name());
        config.setPeriodNameField(InterfaceName.PeriodName.name());
        config.setTransactionIdxes(transactionIdxes);
        step.setConfiguration(JsonUtils.serialize(config));
        previousStep++;
        return step;
    }

    private TransformationStepConfig collectPeriodData(int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_FILTER);
        step.setInputSteps(Arrays.asList(inputStep));

        String tableSourceName = "DailyTable";
        String sourceTableName = dailyTxnStore.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);

        PeriodDataFilterConfig config = new PeriodDataFilterConfig();
        config.setPeriodField(InterfaceName.PeriodId.name());
        config.setPeriodNameField(InterfaceName.PeriodName.name());
        config.setEarliestTransactionDate(MIN_TXN_DATE);
        config.setPeriodStrategies(periodStrategies);
        config.setMultiPeriod(true);
        step.setConfiguration(JsonUtils.serialize(config));
        previousStep++;
        return step;
    }

    private TransformationStepConfig cleanupPeriodHistory(int periodsStep, List<Table> periodTables) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_CLEANER);
        step.setInputSteps(Collections.singletonList(periodsStep));

        List<String> baseSources = new ArrayList<>();
        Map<String, SourceTable> baseTables = new HashMap<>();
        Map<String, Integer> transactionIdxes = new HashMap<>();
        for (int i = 0; i < periodTables.size(); i++) {
            Table periodTable = periodTables.get(i);
            String tableSourceName = "PeriodTable" + periodTable.getName();
            String sourceTableName = periodTable.getName();
            SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
            baseSources.add(tableSourceName);
            baseTables.put(tableSourceName, sourceTable);
            transactionIdxes.put(PeriodStrategyUtils.getPeriodStrategyNameFromPeriodTableName(periodTable.getName(),
                    TableRoleInCollection.ConsolidatedPeriodTransaction), i + 1);
        }
        step.setBaseSources(baseSources);
        step.setBaseTables(baseTables);

        PeriodDataCleanerConfig config = new PeriodDataCleanerConfig();
        config.setPeriodField(InterfaceName.PeriodId.name());
        config.setPeriodNameField(InterfaceName.PeriodName.name());
        config.setTransactionIdxes(transactionIdxes);
        config.setMultiPeriod(true);
        step.setConfiguration(JsonUtils.serialize(config));
        previousStep++;
        return step;
    }

    /**********************************
     * Table Initialization
     **********************************/
    private void initializeTsStores() {
        rawTxnStore = buildTransactionStore(TableRoleInCollection.ConsolidatedRawTransaction).get(0);
        dailyTxnStore = buildTransactionStore(TableRoleInCollection.ConsolidatedDailyTransaction).get(0);
        periodTxnStore = buildTransactionStore(TableRoleInCollection.ConsolidatedPeriodTransaction);
    }

    private List<Table> buildTransactionStore(TableRoleInCollection role) {
        String tableBasePath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace, "")
                .toString();
        List<Table> tables = TransactionUtils.createTxnTables(role, periodStrategies, yarnConfiguration, tableBasePath);
        List<String> tableNames = new ArrayList<>();
        tables.forEach(table -> {
            metadataProxy.updateTable(customerSpace.toString(), table.getName(), table);
            tableNames.add(table.getName());
        });
        return tables;
    }

    /**********************************
     * Input Data Preparation
     **********************************/

    private List<String> actFields = Collections.singletonList(InterfaceName.AccountId.name());
    private List<Class<?>> actClz = Collections.singletonList((Class<?>) String.class);

    private void prepareActTable() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        for (int i = 0; i < actFields.size(); i++) {
            schema.add(Pair.of(actFields.get(i), actClz.get(i)));
        }
        Object[][] data = { { "A1" }, { "A2" }, { "A3" }, { "A4" }, { "A5" } };
        uploadAndRegisterTableSource(schema, data, actTable);
    }

    private List<String> productFields = Arrays.asList(InterfaceName.ProductId.name(), InterfaceName.ProductName.name(),
            InterfaceName.ProductBundle.name(), InterfaceName.ProductLine.name(), InterfaceName.ProductFamily.name(),
            InterfaceName.ProductCategory.name(), InterfaceName.ProductType.name(), InterfaceName.ProductStatus.name());

    private void prepareProductTable() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        for (int i = 0; i < productFields.size(); i++) {
            schema.add(Pair.of(productFields.get(i), String.class));
        }
        Object[][] data = { //
                { "P1", "SKU1", "B1", null, null, null, ProductType.Analytic.name(), ProductStatus.Active.name() }, //
                { "P2", "SKU2", "B2", null, null, null, ProductType.Analytic.name(), ProductStatus.Active.name() }, //
                { "P3", "SKU3", "B3", null, null, null, ProductType.Analytic.name(), ProductStatus.Active.name() }, //
                { "P4", "SKU4", "B4", null, null, null, ProductType.Analytic.name(), ProductStatus.Active.name() }, //
        };
        uploadAndRegisterTableSource(schema, data, productTable);

    }

    private List<String> rawTxnFields = Arrays.asList(InterfaceName.TransactionId.name(),
            InterfaceName.AccountId.name(), InterfaceName.ContactId.name(), InterfaceName.TransactionType.name(),
            InterfaceName.ProductId.name(), InterfaceName.Amount.name(), InterfaceName.Quantity.name(),
            InterfaceName.OrderId.name(), InterfaceName.TransactionTime.name(), "ExtensionAttr1");
    private List<Class<?>> rawTxnClz = Arrays.asList((Class<?>) String.class, String.class, String.class, String.class,
            String.class, Double.class, Long.class, String.class, Long.class, String.class);

    private void prepareRawTxnTablesForRebuild() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        for (int i = 0; i < rawTxnFields.size(); i++) {
            schema.add(Pair.of(rawTxnFields.get(i), rawTxnClz.get(i)));
        }
        // NOTE: TransactionId starts with 1
        // TransactionId, AccountId, ContactId, TransactionType, ProductId,
        // Amount, Quantity, OrderId,
        // TransactionTime, ExtensionAttr1
        Object[][] data = { //
                // (A1, P1) cover multiple week/month/quarter/year.
                // It is to verify aggregation correctness
                // (A1, P1) only exists in rawTxn1
                { "T101", "A1", "C1", ActivityType.PurchaseHistory.name(), "P1", 10.0, 10L, null,
                        strToTimestamp("2017-01-01"), "Ext1" }, //
                { "T102", "A1", "C1", ActivityType.PurchaseHistory.name(), "P1", 10.0, 10L, null,
                        strToTimestamp("2017-02-01"), "Ext1" }, //
                { "T103", "A1", "C2", ActivityType.PurchaseHistory.name(), "P1", 10.0, 10L, null,
                        strToTimestamp("2017-02-01"), "Ext1" }, //
                { "T104", "A1", "C1", ActivityType.PurchaseHistory.name(), "P1", 10.0, 10L, null,
                        strToTimestamp("2017-04-01"), "Ext1" }, //
                { "T105", "A1", "C2", ActivityType.PurchaseHistory.name(), "P1", 10.0, 10L, null,
                        strToTimestamp("2017-04-01"), "Ext1" }, //
                { "T106", "A1", "C3", ActivityType.PurchaseHistory.name(), "P1", 10.0, 10L, null,
                        strToTimestamp("2017-04-01"), "Ext1" }, //
                { "T107", "A1", "C1", ActivityType.PurchaseHistory.name(), "P1", 10.0, 10L, null,
                        strToTimestamp("2018-01-01"), "Ext1" }, //
                { "T108", "A1", "C2", ActivityType.PurchaseHistory.name(), "P1", 10.0, 10L, null,
                        strToTimestamp("2018-01-01"), "Ext1" }, //
                { "T109", "A1", "C3", ActivityType.PurchaseHistory.name(), "P1", 10.0, 10L, null,
                        strToTimestamp("2018-01-01"), "Ext1" }, //
                { "T110", "A1", "C4", ActivityType.PurchaseHistory.name(), "P1", 10.0, 10L, null,
                        strToTimestamp("2018-01-01"), "Ext1" }, //

                // (A1, P2) has txn in both rawTxn1 and rawTxn2 table with same
                // txn date
                { "T111", "A1", "C1", ActivityType.PurchaseHistory.name(), "P2", 10.0, 10L, null,
                        strToTimestamp("2017-07-01"), "Ext1" }, //

                // (A2, P1) only exists in rawTxn2

                // (A2, P2) has txn in both rawTxn1 and rawTxn2 table with
                // different txn date/period
                { "T112", "A2", "C2", ActivityType.PurchaseHistory.name(), "P2", 10.0, 10L, null,
                        strToTimestamp("2017-01-01"), "Ext1" }, //

                // (A3, P3) has txn in both rawTxn1 and rawTxn2 table with
                // different txn date, but same period
                { "T113", "A3", "C3", ActivityType.PurchaseHistory.name(), "P3", 10.0, 10L, null,
                        strToTimestamp("2017-01-01"), "Ext1" }, //

                // (A4, P4) only exists in rawTxn2 to test unseen Account &
                // Product in rawTx

        };

        Object[][] data1 = new Object[data.length / 2][data[0].length];
        Object[][] data2 = new Object[data.length - data1.length][data[0].length];
        for (int i = 0; i < data1.length; i = i + 1) {
            data1[i] = data[i];
        }
        for (int i = 0; i < data2.length; i = i + 1) {
            data2[i] = data[i + data1.length];
        }

        uploadAndRegisterTableSource(schema, data1, rawTxnTable1);
        uploadAndRegisterTableSource(schema, data2, rawTxnTable2);
    }

    private void prepareRawTxnTablesForUpdate() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        for (int i = 0; i < rawTxnFields.size(); i++) {
            schema.add(Pair.of(rawTxnFields.get(i), rawTxnClz.get(i)));
        }
        // NOTE: TransactionId starts with 2
        // TransactionId, AccountId, ContactId, TransactionType, ProductId,
        // Amount, Quantity, OrderId,
        // TransactionTime, ExtensionAttr1
        Object[][] data = { //
                // (A1, P2) has txn in both rawTxn1 and rawTxn2 table with same
                // txn date
                { "T201", "A1", "C1", ActivityType.PurchaseHistory.name(), "P2", 10.0, 10L, null,
                        strToTimestamp("2017-07-01"), "Ext1" }, //

                // (A2, P1) only exists in rawTxn2
                { "T202", "A2", "C1", ActivityType.PurchaseHistory.name(), "P1", 10.0, 10L, null,
                        strToTimestamp("2017-10-01"), "Ext1" }, //

                // (A2, P2) has txn in both rawTxn1 and rawTxn2 table with
                // different txn date/period
                { "T203", "A2", "C2", ActivityType.PurchaseHistory.name(), "P2", 10.0, 10L, null,
                        strToTimestamp("2018-01-01"), "Ext1" }, //

                // (A3, P3) has txn in both rawTxn1 and rawTxn2 table with
                // different txn date, but same period
                { "T204", "A3", "C3", ActivityType.PurchaseHistory.name(), "P3", 10.0, 10L, null,
                        strToTimestamp("2017-01-02"), "Ext1" }, //

                // (A4, P4) only exists in rawTxn2 to test unseen Account &
                // Product
                { "T205", "A4", "C3", ActivityType.PurchaseHistory.name(), "P4", 10.0, 10L, null,
                        strToTimestamp("2017-01-01"), "Ext1" }, //

        };
        uploadAndRegisterTableSource(schema, data, rawTxnTable3);
    }

    private long strToTimestamp(String str) {
        try {
            Date date = new SimpleDateFormat("yyyy-MM-dd").parse(str);
            return date.getTime();
        } catch (ParseException e) {
            throw new RuntimeException("Fail to parse " + str, e);
        }
    }

    /**********************************
     * Result Verification
     **********************************/

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
        }
    }

    /**********************************
     * Other Stuff
     **********************************/

    private void cleanupRegisteredTable(String tableName) {
        metadataProxy.deleteTable(customerSpace.toString(), tableName);
    }

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
    }

    @Override
    protected Source getSource() {
        return source;
    }
}
