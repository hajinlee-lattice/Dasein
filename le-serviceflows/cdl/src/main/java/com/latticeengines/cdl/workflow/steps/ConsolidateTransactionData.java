package com.latticeengines.cdl.workflow.steps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.joda.time.DateTime;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodCollectorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodConvertorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodDataAggregaterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodDataCleanerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodDataDistributorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodDataFilterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodDateConvertorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProductMapperConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ConsolidateTransactionDataStepConfiguration;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.domain.exposed.util.TimeSeriesUtils;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;

@Component("consolidateTransactionData")
public class ConsolidateTransactionData extends ConsolidateDataBase<ConsolidateTransactionDataStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ConsolidateTransactionData.class);

    protected static final String AGGREGATE_TABLE_KEY = "Aggregate";
    protected static final String PERIOD_AGGREGATE_TABLE_KEY = "PeriodAggregate";

    @Autowired
    private YarnConfiguration yarnConfiguration;

    @Autowired
    TransactionTableBuilder transactionTableBuilder;

    @Autowired
    DataFeedProxy dataFeedProxy;

    @Autowired
    DataCollectionProxy dataCollectionProxy;

    private DataCollection.Version activeVersion;
    private DataCollection.Version inactiveVersion;
    private String periodServingStoreTablePrefix;
    private String periodServingStorePrimaryKey;
    private TableRoleInCollection periodServingStore;

    private DataFeed feed;

    private Table rawTable;
    private Table dailyTable;
    private Table periodTable;

    private int inputMergeStep;
    private int dailyStep;
    private int dayPeriodStep;
    private int dailyRawStep;
    private int productAgrStep;
    private int dailyAgrStep;
    private int periodedStep;
    private int periodDataStep;
    private int periodAgrStep;
    private int periodsStep;
    private int retainStep;

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        srcIdField = configuration.getIdField();
        BusinessEntity entity = BusinessEntity.PeriodTransaction;
        periodServingStore = entity.getServingStore();
        if (periodServingStore != null) {
            periodServingStoreTablePrefix = periodServingStore.name();
            periodServingStorePrimaryKey = periodServingStore.getPrimaryKey().name();
        }
        activeVersion = dataCollectionProxy.getActiveVersion(customerSpace.toString());
        inactiveVersion = dataCollectionProxy.getInactiveVersion(customerSpace.toString());
        dataCollectionProxy.getInactiveVersion(customerSpace.toString());
    }

    public PipelineTransformationRequest getConsolidateRequest() {
        try {

            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("ConsolidateTransactionPipeline");

            log.info("current customer space " + customerSpace.toString());
            feed = dataFeedProxy.getDataFeed(customerSpace.toString());
            log.info("Feed says rebuild is " + feed.getRebuildTransaction());

            initializeTsStores();

            inputMergeStep = 0;
            dailyStep = 1;
            dayPeriodStep = 2;
            dailyRawStep = 4;
            productAgrStep = 5;
            periodedStep = 6;
            dailyAgrStep = 7;
            periodsStep = 11;
            periodDataStep = 12;
            periodAgrStep = 13;

            retainStep = 2;

            TransformationStepConfig inputMerge = mergeInputs(true);
            TransformationStepConfig daily = addTrxDate();
            TransformationStepConfig dayPeriods  = collectDays();
            TransformationStepConfig dailyPartition  = partitionDaily();
            TransformationStepConfig report = reportDiff(retainStep);

            log.info("Bucketing " + isBucketing() + " rebuild " + isRebuild());
            boolean rebuild = !isBucketing() || isRebuild();

            Map<String, Product> productMap = null;

            if (!rebuild) {
                Table productTable = dataCollectionProxy.getTable(customerSpace.toString(), TableRoleInCollection.ConsolidatedProduct);
                if (productTable != null) {
                    productMap = TimeSeriesUtils.loadProductMap(yarnConfiguration, productTable);
                }
                if ((productTable == null) || (productMap == null)) {
                    if (productTable == null) {
                        log.info("Rebuild for missing product table");
                    }
                    if (productMap == null) {
                        log.info("Rebuild for missing product map from table" + productTable.getName());
                    }
                    setRebuild();
                    rebuild = true;
                }
            }

            List<TransformationStepConfig> steps = new ArrayList<>();
            steps.add(inputMerge);
            steps.add(daily);
            steps.add(dayPeriods);
            steps.add(dailyPartition);
            if (!rebuild) {
                TransformationStepConfig dailyRaw = collectDailyData();
                TransformationStepConfig productAgr  = rollupProduct(productMap);
                TransformationStepConfig periodAdded = addPeriod();
                TransformationStepConfig dailyAgr  = aggregateDaily();
                TransformationStepConfig dailyRetained = retainFields(dailyAgrStep, false);
                TransformationStepConfig cleanDaily  = cleanupDailyHistory();
                TransformationStepConfig updateDaily  = updateDailyStore();
                TransformationStepConfig periods  = collectPeriods();
                TransformationStepConfig periodData = collectPeriodData();
                TransformationStepConfig periodAgr  = aggregatePeriods();
                TransformationStepConfig periodRetained  = retainFields(periodAgrStep, false, TableRoleInCollection.AggregatedPeriodTransaction);
                TransformationStepConfig cleanPeriod  = cleanupPeriodHistory();
                TransformationStepConfig updatePeriod  = updatePeriodStore();

                steps.add(dailyRaw);
                steps.add(productAgr);
                steps.add(periodAdded);
                steps.add(dailyAgr);
                steps.add(dailyRetained);
                steps.add(cleanDaily);
                steps.add(updateDaily);
                steps.add(periods);
                steps.add(periodData);
                steps.add(periodAgr);
                steps.add(periodRetained);
                steps.add(cleanPeriod);
                steps.add(updatePeriod);

            }
            steps.add(report);
            request.setSteps(steps);
            return request;

        } catch (Exception e) {
            log.error("Failed to run consolidate data pipeline!", e);
            throw new RuntimeException(e);
        }
    }

    private TransformationStepConfig addTrxDate() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATE_CONVERTOR);
        step.setInputSteps(Collections.singletonList(inputMergeStep));
        PeriodDateConvertorConfig config = new PeriodDateConvertorConfig();
        config.setTrxTimeField(InterfaceName.TransactionTime.name());
        config.setTrxDateField(InterfaceName.TransactionDate.name());
        config.setTrxDayPeriodField(InterfaceName.TransactionDayPeriod.name());
        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }


    private TransformationStepConfig collectDays() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_COLLECTOR);
        step.setInputSteps(Collections.singletonList(dailyStep));
        PeriodCollectorConfig config = new PeriodCollectorConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());
        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }


    private TransformationStepConfig partitionDaily() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_DISTRIBUTOR);
        List<Integer> inputSteps = new ArrayList<Integer>();
        inputSteps.add(dayPeriodStep);
        inputSteps.add(dailyStep);
        step.setInputSteps(inputSteps);

        String tableSourceName = "CustomerUniverse";
        String sourceTableName = rawTable.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);

        PeriodDataDistributorConfig config = new PeriodDataDistributorConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());
        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private TransformationStepConfig collectDailyData() {
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
        config.setEarliestTransactionDate(DateTimeUtils.dayPeriodToDate(feed.getEarliestTransaction()));
        step2.setConfiguration(JsonUtils.serialize(config));
        return step2;
    }

    private TransformationStepConfig rollupProduct(Map<String, Product> productMap) {
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PRODUCT_MAPPER);
        step2.setInputSteps(Collections.singletonList(dailyRawStep));
        ProductMapperConfig config = new ProductMapperConfig();
        config.setProductField(InterfaceName.ProductId.name());
        config.setProductMap(productMap);

        step2.setConfiguration(JsonUtils.serialize(config));
        return step2;
    }

    private TransformationStepConfig addPeriod() {
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PERIOD_CONVERTOR);
        step2.setInputSteps(Collections.singletonList(productAgrStep));
        PeriodConvertorConfig config = new PeriodConvertorConfig();
        config.setTrxDateField(InterfaceName.TransactionDate.name());
        config.setPeriodStrategy(configuration.getPeriodStrategy());
        config.setPeriodField(InterfaceName.PeriodId.name());
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

    private TransformationStepConfig aggregateDaily() {
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PERIOD_DATA_AGGREGATER);
        step2.setInputSteps(Collections.singletonList(periodedStep));
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(servingStoreTablePrefix);
        targetTable.setPrimaryKey(servingStorePrimaryKey);
        targetTable.setExpandBucketedAttrs(false);
        step2.setTargetTable(targetTable);
        PeriodDataAggregaterConfig config = new PeriodDataAggregaterConfig();
        config.setSumFields(Arrays.asList("Amount"));
        config.setSumOutputFields(Arrays.asList("TotalAmount"));
        config.setSumLongFields(Arrays.asList("Quantity"));
        config.setSumLongOutputFields(Arrays.asList("TotalQuantity"));
        config.setGoupByFields(Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.ContactId.name(),
                InterfaceName.ProductId.name(), InterfaceName.TransactionType.name(),
                InterfaceName.TransactionDate.name(),
                InterfaceName.PeriodId.name(),
                InterfaceName.TransactionDayPeriod.name()));
        step2.setConfiguration(JsonUtils.serialize(config));
        return step2;
    }

    private TransformationStepConfig updateDailyStore() {
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PERIOD_DATA_DISTRIBUTOR);
        List<Integer> inputSteps = new ArrayList<Integer>();
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

    private TransformationStepConfig aggregatePeriods() {
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PERIOD_DATA_AGGREGATER);
        step2.setInputSteps(Collections.singletonList(periodDataStep));
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(periodServingStoreTablePrefix);
        targetTable.setPrimaryKey(periodServingStorePrimaryKey);
        targetTable.setExpandBucketedAttrs(false);
        step2.setTargetTable(targetTable);
        PeriodDataAggregaterConfig config = new PeriodDataAggregaterConfig();
        config.setSumFields(Arrays.asList("TotalAmount"));
        config.setSumOutputFields(Arrays.asList("TotalAmount"));
        config.setSumLongFields(Arrays.asList("TotalQuantity"));
        config.setSumLongOutputFields(Arrays.asList("TotalQuantity"));
        config.setGoupByFields(Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.ContactId.name(),
                InterfaceName.ProductId.name(), InterfaceName.TransactionType.name(),
                InterfaceName.PeriodId.name()));
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

    private TransformationStepConfig collectPeriodData() {
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PERIOD_DATA_FILTER);
        step2.setInputSteps(Collections.singletonList(periodsStep));

        String tableSourceName = "CustomerUniverse";
        String sourceTableName = dailyTable.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step2.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step2.setBaseTables(baseTables);

        PeriodDataFilterConfig config = new PeriodDataFilterConfig();
        config.setPeriodField(InterfaceName.PeriodId.name());
        config.setPeriodStrategy(configuration.getPeriodStrategy());
        config.setEarliestTransactionDate(DateTimeUtils.dayPeriodToDate(feed.getEarliestTransaction()));
        step2.setConfiguration(JsonUtils.serialize(config));
        return step2;
    }

    private TransformationStepConfig cleanupPeriodHistory() {
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
        List<Integer> inputSteps = new ArrayList<Integer>();
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

    @Override
    protected void onPostTransformationCompleted() {

        if (isBucketing() && !isRebuild()) {
            publishTable(BusinessEntity.Transaction, servingStoreTablePrefix, AGGREGATE_TABLE_KEY);
            publishTable(BusinessEntity.PeriodTransaction, periodServingStoreTablePrefix, PERIOD_AGGREGATE_TABLE_KEY);
         }

        Integer earliestDayPeriod = TimeSeriesUtils.getEarliestPeriod(yarnConfiguration, rawTable);
        Integer currentEarliest = feed.getEarliestTransaction();
        if ((currentEarliest == null) || (earliestDayPeriod < currentEarliest)) {
            dataFeedProxy.updateEarliestTransaction(customerSpace.toString(), earliestDayPeriod);
            setRebuild();
        }

        super.onPostTransformationCompleted(false);
    }

    private void enrichTableSchema(Table table) {
        List<Attribute> attrs = table.getAttributes();
        attrs.forEach(Attribute::removeAllowedDisplayNames);
    }

    private void initializeTsStores() {
        boolean rebuildTransaction = false;
        rawTable = dataCollectionProxy.getTable(customerSpace.toString(), TableRoleInCollection.ConsolidatedRawTransaction);
        if (rawTable == null) {
            log.info("Create raw table");
            rawTable = buildPeriodStore(TableRoleInCollection.ConsolidatedRawTransaction, SchemaInterpretation.TransactionRaw, activeVersion, inactiveVersion);
            rebuildTransaction = true;
        }
        dailyTable = dataCollectionProxy.getTable(customerSpace.toString(), TableRoleInCollection.ConsolidatedDailyTransaction, activeVersion);
        if (dailyTable == null) {
            log.info("Create daily table");
            dailyTable = buildPeriodStore(TableRoleInCollection.ConsolidatedDailyTransaction, SchemaInterpretation.TransactionDailyAggregation, activeVersion, null);
            rebuildTransaction = true;
        }
        periodTable = dataCollectionProxy.getTable(customerSpace.toString(), TableRoleInCollection.ConsolidatedPeriodTransaction, activeVersion);
        if (periodTable == null) {
            log.info("Create period table");
            periodTable = buildPeriodStore(TableRoleInCollection.ConsolidatedPeriodTransaction, SchemaInterpretation.TransactionPeriodAggregation, activeVersion, null);
            rebuildTransaction = true;
        }

        if (rebuildTransaction) {
            setRebuild();
        }
    }

    private Table buildPeriodStore(TableRoleInCollection role, SchemaInterpretation schema, DataCollection.Version version, DataCollection.Version version2) {
        Table table = SchemaRepository.instance().getSchema(schema);
        String hdfsPath = PathBuilder
                .buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace, "").toString();
        try {
            log.info("Initialize period store " + hdfsPath + "/" + schema);
            HdfsUtils.mkdir(yarnConfiguration, hdfsPath + "/" + schema);
        } catch (Exception e) {
            log.error("Failed to initialize period store " + hdfsPath + "/" + schema);
            throw new RuntimeException("Failed to create period store " + role);
        }

        Extract extract = new Extract();
        extract.setName("extract_target");
        extract.setExtractionTimestamp(DateTime.now().getMillis());
        extract.setProcessedRecords(1L);
        extract.setPath(hdfsPath + "/" + schema + "/");
        table.setExtracts(Collections.singletonList(extract));
        metadataProxy.updateTable(customerSpace.toString(), table.getName(), table);
        dataCollectionProxy.upsertTable(customerSpace.toString(), table.getName(), role, version);
        log.info("Upsert table " + table.getName() + " to role " + role + "version" + version.name());

        if (version2 != null) {
            Table inactiveTable = SchemaRepository.instance().getSchema(schema);
            inactiveTable.setName(inactiveTable.getName() + "2");
            inactiveTable.setExtracts(Collections.singletonList(extract));
            metadataProxy.updateTable(customerSpace.toString(), inactiveTable.getName(), inactiveTable);
            dataCollectionProxy.upsertTable(customerSpace.toString(), inactiveTable.getName(), role, version2);
            log.info("Upsert table " + inactiveTable.getName() + " to role " + role + "version" + version2.name());
        }
        return table;
    }

    private void publishTable(BusinessEntity entity, String tablePrefix, String contextKey) {

        String redshiftTableName = TableUtils.getFullTableName(tablePrefix, pipelineVersion);
        Table redshiftTable = metadataProxy.getTable(customerSpace.toString(), redshiftTableName);
        if (redshiftTable == null) {
            throw new RuntimeException("Diff table has not been created.");
        }
        enrichTableSchema(redshiftTable);
        metadataProxy.updateTable(configuration.getCustomerSpace().toString(), redshiftTableName, redshiftTable);
        Map<BusinessEntity, String> entityTableMap = getMapObjectFromContext(TABLE_GOING_TO_REDSHIFT,
            BusinessEntity.class, String.class);
        if (entityTableMap == null) {
            entityTableMap = new HashMap<>();
        }
        entityTableMap.put(entity, redshiftTableName);
        putObjectInContext(TABLE_GOING_TO_REDSHIFT, entityTableMap);

        Map<BusinessEntity, Boolean> appendTableMap = getMapObjectFromContext(APPEND_TO_REDSHIFT_TABLE,
            BusinessEntity.class, Boolean.class);
        if (appendTableMap == null) {
            appendTableMap = new HashMap<>();
        }
        appendTableMap.put(entity, true);
        putObjectInContext(APPEND_TO_REDSHIFT_TABLE, appendTableMap);
        putObjectInContext(contextKey, redshiftTable);
    }

    private boolean isRebuild() {
        Boolean rebuildTransaction = feed.getRebuildTransaction();
        if (rebuildTransaction == null) {
            log.info("First init rebuild transaction to TRUE");
            feed.setRebuildTransaction(Boolean.TRUE);
            dataFeedProxy.rebuildTransaction(customerSpace.toString(), Boolean.TRUE);
            return true;
        }
        return rebuildTransaction.booleanValue();
    }

    private void setRebuild() {
        if (!isRebuild()) {
            feed.setRebuildTransaction(Boolean.TRUE);
            dataFeedProxy.rebuildTransaction(customerSpace.toString(), Boolean.TRUE);
        }
    }

    @Override
    protected void setupConfig(ConsolidateDataTransformerConfig config) {
        config.setMasterIdField(InterfaceName.TransactionId.name());
    }

    @Override
    protected String getMergeTableName() {
        return servingStore.name() + "_Merged";
    }
}
