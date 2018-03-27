package com.latticeengines.cdl.workflow.steps.update;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_CONSOLIDATE_RETAIN;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.utils.PeriodStrategyUtils;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.domain.exposed.util.ProductUtils;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateRetainFieldConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodCollectorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodConvertorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodDataAggregaterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodDataCleanerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodDataDistributorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodDataFilterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProductMapperConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;

@Component(ProcessTransactionDiff.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProcessTransactionDiff extends BaseProcessDiffStep<ProcessTransactionStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ProcessTransactionDiff.class);

    static final String BEAN_NAME = "processTransactionDiff";

    private Map<String, List<Product>> productMap;
    private int dailyRawStep, productAgrStep, addPeriodStep;
    private int dailyAgrStep, periodsStep, periodDataStep, periodDataWithPeriodIdStep, periodAgrStep;

    private Table rawTable, dailyTable;
    private List<Table> periodTables;
    private String dailyTablePrefix, periodTablePrefix, servingStorePrimaryKey;
    private String earliestTransaction;
    private String diffTableName;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private PeriodProxy periodProxy;

    @Inject
    private Configuration yarnConfiguration;

    private List<PeriodStrategy> periodStrategies;

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();

        dailyTablePrefix = TableRoleInCollection.AggregatedTransaction.name();
        periodTablePrefix = TableRoleInCollection.AggregatedPeriodTransaction.name();
        servingStorePrimaryKey = InterfaceName.__Composite_Key__.name();

        periodStrategies = periodProxy.getPeriodStrategies(customerSpace.toString());

        rawTable = dataCollectionProxy.getTable(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedRawTransaction, inactive);
        dailyTable = dataCollectionProxy.getTable(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedDailyTransaction, inactive);
        periodTables = dataCollectionProxy.getTables(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedPeriodTransaction, inactive);

        Map<BusinessEntity, String> diffTableNames = getMapObjectFromContext(ENTITY_DIFF_TABLES, BusinessEntity.class,
                String.class);
        diffTableName = diffTableNames.get(BusinessEntity.Transaction);

        DataFeed feed = dataFeedProxy.getDataFeed(customerSpace.toString());
        earliestTransaction = DateTimeUtils.dayPeriodToDate(feed.getEarliestTransaction());

        loadProductMap();
    }

    @Override
    protected void onPostTransformationCompleted() {
        String sortedDailyTableName = TableUtils.getFullTableName(dailyTablePrefix, pipelineVersion);
        String sortedPeriodTableName = TableUtils.getFullTableName(periodTablePrefix, pipelineVersion);
        if (metadataProxy.getTable(customerSpace.toString(), sortedDailyTableName) == null) {
            throw new IllegalStateException("Cannot find result sorted daily table");
        }
        if (metadataProxy.getTable(customerSpace.toString(), sortedPeriodTableName) == null) {
            throw new IllegalStateException("Cannot find result sorted period table");
        }
        updateEntityValueMapInContext(BusinessEntity.Transaction, TABLE_GOING_TO_REDSHIFT, sortedDailyTableName,
                String.class);
        updateEntityValueMapInContext(BusinessEntity.Transaction, APPEND_TO_REDSHIFT_TABLE, true, Boolean.class);
        updateEntityValueMapInContext(BusinessEntity.PeriodTransaction, TABLE_GOING_TO_REDSHIFT, sortedPeriodTableName,
                String.class);
        updateEntityValueMapInContext(BusinessEntity.PeriodTransaction, APPEND_TO_REDSHIFT_TABLE, true, Boolean.class);
    }

    @Override
    protected PipelineTransformationRequest getTransformRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("ProcessTransactionDiff");
        List<TransformationStepConfig> steps = new ArrayList<>();

        dailyRawStep = 0;
        productAgrStep = 1;
        addPeriodStep = 2;
        dailyAgrStep = 3;

        TransformationStepConfig dailyRaw = collectDailyData(); // dailyRawStep
        TransformationStepConfig productAgr = rollupProduct(productMap); // productAgrStep
        TransformationStepConfig periodAdded = addPeriod(productAgrStep, null); // addPeriodStep
        TransformationStepConfig dailyAgr = aggregateDaily(); // dailyAgrStep
        TransformationStepConfig dailyRetained = retainFields(dailyAgrStep,
                TableRoleInCollection.AggregatedTransaction);
        TransformationStepConfig cleanDaily = cleanupDailyHistory();
        TransformationStepConfig updateDaily = updateDailyStore();

        steps.add(dailyRaw);
        steps.add(productAgr);
        steps.add(periodAdded);
        steps.add(dailyAgr);
        steps.add(dailyRetained);
        steps.add(cleanDaily);
        steps.add(updateDaily);

        addPeriodStep = 7;
        periodsStep = 8;
        periodDataStep = 9;
        periodDataWithPeriodIdStep = 10;
        periodAgrStep = 11;

        List<Integer> periodAgrSteps = new ArrayList<>();
        for (PeriodStrategy strategy : periodStrategies) {
            periodAdded = addPeriod(dailyAgrStep, strategy); // addPeriodStep
            TransformationStepConfig periods = collectPeriods(addPeriodStep); // periodsStep
            TransformationStepConfig periodData = collectPeriodData(strategy, periodsStep); // periodDataStep
            TransformationStepConfig periodDataWithPeriodId = addPeriod(periodDataStep, strategy); // periodDataWithPeriodIdStep
            TransformationStepConfig periodAgr = aggregatePeriods(strategy, periodDataWithPeriodIdStep); // periodAgrStep
            TransformationStepConfig periodRetained = retainFields(periodAgrStep,
                    TableRoleInCollection.AggregatedPeriodTransaction);
            TransformationStepConfig cleanPeriod = cleanupPeriodHistory(periodsStep,
                    PeriodStrategyUtils.findPeriodTableFromStrategy(periodTables, strategy));
            TransformationStepConfig updatePeriod = updatePeriodStore(periodsStep, periodAgrStep,
                    PeriodStrategyUtils.findPeriodTableFromStrategy(periodTables, strategy));

            steps.add(periodAdded);
            steps.add(periods);
            steps.add(periodData);
            steps.add(periodDataWithPeriodId);
            steps.add(periodAgr);
            steps.add(periodRetained);
            steps.add(cleanPeriod);
            steps.add(updatePeriod);

            periodAgrSteps.add(periodAgrStep);

            addPeriodStep += 8;
            periodsStep += 8;
            periodDataStep += 8;
            periodDataWithPeriodIdStep += 8;
            periodAgrStep += 8;

        }

        TransformationStepConfig mergePeriod = mergePeriodStore(periodAgrSteps);
        steps.add(mergePeriod);

        request.setSteps(steps);
        return request;
    }

    private void loadProductMap() {
        Table productTable = dataCollectionProxy.getTable(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedProduct);
        if (productTable == null) {
            throw new IllegalStateException("Cannot find the product table in default collection");
        }
        log.info(String.format("productTableName for customer %s is %s", configuration.getCustomerSpace().toString(),
                productTable.getName()));
//        productMap = TimeSeriesUtils.loadProductMap(yarnConfiguration, productTable);
        List<Product> productList = new ArrayList<>();
        productTable.getExtracts().forEach(extract ->
            productList.addAll(ProductUtils.loadProducts(yarnConfiguration, extract.getPath())));
        productMap = ProductUtils.getActiveProductMap(productList);
    }

    private TransformationStepConfig collectDailyData() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_FILTER);

        String rawDiffSource = "RawDiff";
        SourceTable diffSourceTable = new SourceTable(diffTableName, customerSpace);
        String rawTableSource = "RawMaster";
        SourceTable masterSourceTable = new SourceTable(rawTable.getName(), customerSpace);
        List<String> baseSources = Arrays.asList(rawDiffSource, rawTableSource);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(rawDiffSource, diffSourceTable);
        baseTables.put(rawTableSource, masterSourceTable);
        step.setBaseTables(baseTables);

        PeriodDataFilterConfig config = new PeriodDataFilterConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());
        config.setEarliestTransactionDate(earliestTransaction);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig rollupProduct(Map<String, List<Product>> productMap) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PRODUCT_MAPPER);
        step.setInputSteps(Collections.singletonList(dailyRawStep));
        ProductMapperConfig config = new ProductMapperConfig();
        config.setProductField(InterfaceName.ProductId.name());
        config.setProductTypeField(InterfaceName.ProductType.name());
        config.setProductMap(productMap);

        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig addPeriod(int inputStep, PeriodStrategy strategy) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_CONVERTOR);
        step.setInputSteps(Collections.singletonList(inputStep));
        PeriodConvertorConfig config = new PeriodConvertorConfig();
        config.setTrxDateField(InterfaceName.TransactionDate.name());
        config.setPeriodStrategy(strategy);
        config.setPeriodField(InterfaceName.PeriodId.name());
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig cleanupDailyHistory() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_CLEANER);

        String sourceName1 = "DailyPeriodTable";
        SourceTable sourceTable1 = new SourceTable(diffTableName, customerSpace);
        String sourceName2 = "DailyTable";
        SourceTable sourceTable2 = new SourceTable(dailyTable.getName(), customerSpace);
        List<String> baseSources = Arrays.asList(sourceName1, sourceName2);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(sourceName1, sourceTable1);
        baseTables.put(sourceName2, sourceTable2);

        step.setBaseTables(baseTables);
        step.setInputSteps(Collections.singletonList(productAgrStep));
        PeriodDataCleanerConfig config = new PeriodDataCleanerConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig aggregateDaily() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_AGGREGATER);
        step.setInputSteps(Collections.singletonList(addPeriodStep));
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(dailyTablePrefix);
        targetTable.setPrimaryKey(servingStorePrimaryKey);
        targetTable.setExpandBucketedAttrs(false);
        step.setTargetTable(targetTable);
        PeriodDataAggregaterConfig config = new PeriodDataAggregaterConfig();
        config.setSumFields(Arrays.asList(
                InterfaceName.Amount.name(),
                InterfaceName.Cost.name()));
        config.setSumOutputFields(Arrays.asList(
                InterfaceName.TotalAmount.name(),
                InterfaceName.TotalCost.name()));
        config.setSumLongFields(Collections.singletonList(InterfaceName.Quantity.name()));
        config.setSumLongOutputFields(Collections.singletonList(InterfaceName.TotalQuantity.name()));
        config.setCountField(Collections.singletonList(InterfaceName.TransactionTime.name()));
        config.setCountOutputField(Collections.singletonList(InterfaceName.TransactionCount.name()));
        config.setGroupByFields(Arrays.asList( //
                InterfaceName.AccountId.name(), //
                InterfaceName.ContactId.name(), //
                InterfaceName.ProductId.name(), //
                InterfaceName.TransactionType.name(), //
                InterfaceName.TransactionDate.name(), //
                InterfaceName.TransactionDayPeriod.name()));
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig updateDailyStore() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_DISTRIBUTOR);
        step.setInputSteps(Collections.singletonList(dailyAgrStep));

        String sourceName1 = "DailyPeriodTable";
        SourceTable sourceTable1 = new SourceTable(diffTableName, customerSpace);
        String sourceName2 = "DailyTable";
        SourceTable sourceTable2 = new SourceTable(dailyTable.getName(), customerSpace);
        step.setBaseSources(Arrays.asList(sourceName1, sourceName2));
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(sourceName1, sourceTable1);
        baseTables.put(sourceName2, sourceTable2);
        step.setBaseTables(baseTables);

        PeriodDataDistributorConfig config = new PeriodDataDistributorConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());
        config.setInputIdx(0);
        config.setPeriodIdx(1);
        config.setTransactinIdx(2);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig aggregatePeriods(PeriodStrategy strategy, int periodDataWithPeriodIdStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_AGGREGATER);
        step.setInputSteps(Collections.singletonList(periodDataWithPeriodIdStep));
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(periodTablePrefix + strategy.getName());
        targetTable.setPrimaryKey(servingStorePrimaryKey);
        targetTable.setExpandBucketedAttrs(false);
        step.setTargetTable(targetTable);
        PeriodDataAggregaterConfig config = new PeriodDataAggregaterConfig();
        config.setPeriodStrategy(strategy);
        config.setSumFields(Arrays.asList(
                InterfaceName.TotalAmount.name(),
                InterfaceName.TotalCost.name(),
                InterfaceName.TransactionCount.name()));
        config.setSumOutputFields(Arrays.asList(
                InterfaceName.TotalAmount.name(),
                InterfaceName.TotalCost.name(),
                InterfaceName.TransactionCount.name()));
        config.setSumLongFields(Collections.singletonList(InterfaceName.TotalQuantity.name()));
        config.setSumLongOutputFields(Collections.singletonList(InterfaceName.TotalQuantity.name()));
        config.setGroupByFields(Arrays.asList( //
                InterfaceName.AccountId.name(), //
                InterfaceName.ContactId.name(), //
                InterfaceName.ProductId.name(), //
                InterfaceName.TransactionType.name(), //
                InterfaceName.PeriodId.name()));
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig collectPeriods(int addPeriodStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_COLLECTOR);
        step.setInputSteps(Collections.singletonList(addPeriodStep));
        PeriodCollectorConfig config = new PeriodCollectorConfig();
        config.setPeriodField(InterfaceName.PeriodId.name());
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig collectPeriodData(PeriodStrategy strategy, int periodsStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_FILTER);
        step.setInputSteps(Arrays.asList(periodsStep));


        String tableSourceName = "DailyTable";
        String sourceTableName = dailyTable.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);


        PeriodDataFilterConfig config = new PeriodDataFilterConfig();
        config.setPeriodField(InterfaceName.PeriodId.name());
        config.setPeriodStrategy(strategy);
        config.setEarliestTransactionDate(earliestTransaction);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig cleanupPeriodHistory(int periodsStep, Table periodTable) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_CLEANER);
        step.setInputSteps(Collections.singletonList(periodsStep));

        String tableSourceName = "PeriodTable";
        String sourceTableName = periodTable.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);
        PeriodDataCleanerConfig config = new PeriodDataCleanerConfig();
        config.setPeriodField(InterfaceName.PeriodId.name());
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig updatePeriodStore(int periodsStep, int periodAgrStep, Table periodTable) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_DISTRIBUTOR);
        List<Integer> inputSteps = new ArrayList<>();
        inputSteps.add(periodsStep);
        inputSteps.add(periodAgrStep);
        step.setInputSteps(inputSteps);

        String tableSourceName = "PeriodTable";
        String sourceTableName = periodTable.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);

        PeriodDataDistributorConfig config = new PeriodDataDistributorConfig();
        config.setPeriodField(InterfaceName.PeriodId.name());
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig retainFields(int previousStep, TableRoleInCollection role) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(previousStep));
        step.setTransformer(TRANSFORMER_CONSOLIDATE_RETAIN);

        ConsolidateRetainFieldConfig config = new ConsolidateRetainFieldConfig();
        Table servingTable = dataCollectionProxy.getTable(customerSpace.toString(), role);
        if (servingTable != null) {
            List<String> fieldsToRetain = AvroUtils.getSchemaFields(yarnConfiguration,
                    servingTable.getExtracts().get(0).getPath());
            config.setFieldsToRetain(fieldsToRetain);
        }
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig mergePeriodStore(List<Integer> periodAgrSteps) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.TRANSFORMER_MERGE);

        step.setInputSteps(periodAgrSteps);
        
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(periodTablePrefix);
        targetTable.setPrimaryKey(servingStorePrimaryKey);
        targetTable.setExpandBucketedAttrs(false);
        step.setTargetTable(targetTable);

        step.setConfiguration(emptyStepConfig(lightEngineConfig()));
        return step;
    }

}
