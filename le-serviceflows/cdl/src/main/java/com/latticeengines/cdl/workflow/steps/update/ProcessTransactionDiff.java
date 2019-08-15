package com.latticeengines.cdl.workflow.steps.update;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodCollectorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodConvertorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodDataAggregaterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodDataDistributorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodDataFilterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ProductMapperConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.util.PeriodStrategyUtils;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.serviceflows.workflow.util.ScalingUtils;

@Component(ProcessTransactionDiff.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProcessTransactionDiff extends BaseProcessDiffStep<ProcessTransactionStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ProcessTransactionDiff.class);

    static final String BEAN_NAME = "processTransactionDiff";

    private Table productTable;
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

    private List<PeriodStrategy> periodStrategies;

    private boolean entityMatchEnabled;

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

        Table diffTable = metadataProxy.getTable(customerSpace.toString(), diffTableName);
        if (diffTable == null) {
            throw new RuntimeException("Cannot find diff txn table " + diffTableName);
        }
        double sizeInGb = ScalingUtils.getTableSizeInGb(yarnConfiguration, diffTable);
        int multiplier = ScalingUtils.getMultiplier(sizeInGb);
        log.info("Set scalingMultiplier=" + multiplier + " base on diff txn table size=" + sizeInGb + " gb.");
        scalingMultiplier = multiplier;

        DataFeed feed = dataFeedProxy.getDataFeed(customerSpace.toString());
        earliestTransaction = DateTimeUtils.dayPeriodToDate(feed.getEarliestTransaction());

        loadProductMap();

        entityMatchEnabled = configuration.isEntityMatchEnabled();
        if (entityMatchEnabled) {
            log.info("Entity match is enabled for transaction update");
        }
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
        Map<TableRoleInCollection, String> processedTableNames = getMapObjectFromContext(PROCESSED_DIFF_TABLES, //
                TableRoleInCollection.class, String.class);
        if (processedTableNames == null) {
            processedTableNames = new HashMap<>();
        }
        processedTableNames.put(BusinessEntity.Transaction.getServingStore(), sortedDailyTableName);
        processedTableNames.put(BusinessEntity.PeriodTransaction.getServingStore(), sortedPeriodTableName);
        putObjectInContext(PROCESSED_DIFF_TABLES, processedTableNames);
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
        TransformationStepConfig productAgr = rollupProduct(productTable); // productAgrStep
        TransformationStepConfig periodAdded = addPeriod(productAgrStep, null); // addPeriodStep
        TransformationStepConfig dailyAgr = aggregateDaily(); // dailyAgrStep
        TransformationStepConfig dailyRetained = retainFields(dailyAgrStep, //
                TableRoleInCollection.AggregatedTransaction);
        TransformationStepConfig updateDaily = updateDailyStore();

        steps.add(dailyRaw);
        steps.add(productAgr);
        steps.add(periodAdded);
        steps.add(dailyAgr);
        steps.add(dailyRetained);
        steps.add(updateDaily);

        addPeriodStep = 6;
        periodsStep = 7;
        periodDataStep = 8;
        periodDataWithPeriodIdStep = 9;
        periodAgrStep = 10;
        periodAdded = addPeriod(dailyAgrStep, periodStrategies); // periodedStep
        TransformationStepConfig periods = collectPeriods(addPeriodStep); // periodsStep
        TransformationStepConfig periodData = collectPeriodData(periodsStep); // periodDataStep
        TransformationStepConfig periodDataWithPeriodId = addPeriod(periodDataStep, periodStrategies); // periodDataWithPeriodIdStep
        TransformationStepConfig periodAgr = aggregatePeriods(periodDataWithPeriodIdStep); // periodAgrStep
        TransformationStepConfig periodRetained = retainFields(periodAgrStep, periodTablePrefix, //
                servingStorePrimaryKey, TableRoleInCollection.AggregatedPeriodTransaction);
        TransformationStepConfig updatePeriod = updatePeriodStore(periodTables, periodsStep, periodAgrStep);

        steps.add(periodAdded);
        steps.add(periods);
        steps.add(periodData);
        steps.add(periodDataWithPeriodId);
        steps.add(periodAgr);
        steps.add(periodRetained);
        steps.add(updatePeriod);

        request.setSteps(steps);
        return request;
    }

    private void loadProductMap() {
        productTable = dataCollectionProxy.getTable(customerSpace.toString(), TableRoleInCollection.ConsolidatedProduct,
                inactive);
        if (productTable == null) {
            log.info("Did not find product table in inactive version.");
            productTable = dataCollectionProxy.getTable(customerSpace.toString(),
                    TableRoleInCollection.ConsolidatedProduct, active);
            if (productTable == null) {
                throw new IllegalStateException("Cannot find the product table in both versions");
            }
        }
        log.info(String.format("productTableName for customer %s is %s", configuration.getCustomerSpace().toString(),
                productTable.getName()));
    }

    private TransformationStepConfig collectDailyData() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_FILTER);
        addBaseTables(step, diffTableName);
        addBaseTables(step, rawTable.getName());

        PeriodDataFilterConfig config = new PeriodDataFilterConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());
        config.setEarliestTransactionDate(earliestTransaction);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig rollupProduct(Table productTable) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PRODUCT_MAPPER);
        step.setInputSteps(Collections.singletonList(dailyRawStep));

        String productTableName = productTable.getName();
        List<String> baseSources = Collections.singletonList(productTableName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(productTableName, new SourceTable(productTableName, customerSpace));
        step.setBaseTables(baseTables);

        ProductMapperConfig config = new ProductMapperConfig();
        config.setProductField(InterfaceName.ProductId.name());
        config.setProductTypeField(InterfaceName.ProductType.name());

        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig addPeriod(int inputStep, List<PeriodStrategy> periodStrategies) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_CONVERTOR);
        step.setInputSteps(Collections.singletonList(inputStep));
        PeriodConvertorConfig config = new PeriodConvertorConfig();
        config.setTrxDateField(InterfaceName.TransactionDate.name());
        config.setPeriodStrategies(periodStrategies);
        config.setPeriodField(InterfaceName.PeriodId.name());
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig aggregateDaily() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_AGGREGATER);
        step.setInputSteps(Collections.singletonList(addPeriodStep));
        setTargetTable(step, dailyTablePrefix, servingStorePrimaryKey);

        PeriodDataAggregaterConfig config = new PeriodDataAggregaterConfig();
        config.setSumFields(
                Arrays.asList(InterfaceName.Amount.name(), InterfaceName.Cost.name(), InterfaceName.Quantity.name()));
        config.setSumOutputFields(Arrays.asList(InterfaceName.TotalAmount.name(), InterfaceName.TotalCost.name(),
                InterfaceName.TotalQuantity.name()));
        config.setCountField(Collections.singletonList(InterfaceName.TransactionTime.name()));
        config.setCountOutputField(Collections.singletonList(InterfaceName.TransactionCount.name()));
        List<String> groupByFields = new ArrayList<>();
        if (entityMatchEnabled) {
            // In the future, Transaction could have more account fields, need
            // to consider:
            // 1. Are they needed in transaction store
            // 2. How to properly and efficiently retain them -- Keeping adding
            // in group fields could have performance concern; Add a join?
            groupByFields.add(InterfaceName.CustomerAccountId.name());
        }
        groupByFields.addAll(Arrays.asList( //
                InterfaceName.AccountId.name(), //
                InterfaceName.ProductId.name(), //
                InterfaceName.ProductType.name(), //
                InterfaceName.TransactionType.name(), //
                InterfaceName.TransactionDate.name(), //
                InterfaceName.TransactionDayPeriod.name()));
        config.setGroupByFields(groupByFields);
        step.setConfiguration(appendEngineConf(config, heavyMemoryEngineConfig()));
        return step;
    }

    private TransformationStepConfig updateDailyStore() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_DISTRIBUTOR);
        step.setInputSteps(Collections.singletonList(dailyAgrStep));
        addBaseTables(step, diffTableName);
        addBaseTables(step, dailyTable.getName());

        PeriodDataDistributorConfig config = new PeriodDataDistributorConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());
        config.setInputIdx(0);
        config.setPeriodIdx(1);
        config.setTransactinIdx(2);
        config.setRetryable(true);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig aggregatePeriods(int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_AGGREGATER);
        step.setInputSteps(Collections.singletonList(inputStep));

        PeriodDataAggregaterConfig config = new PeriodDataAggregaterConfig();
        config.setSumFields(Arrays.asList(InterfaceName.TotalAmount.name(), InterfaceName.TotalCost.name(),
                InterfaceName.TransactionCount.name(), InterfaceName.TotalQuantity.name()));
        config.setSumOutputFields(Arrays.asList(InterfaceName.TotalAmount.name(), InterfaceName.TotalCost.name(),
                InterfaceName.TransactionCount.name(), InterfaceName.TotalQuantity.name()));
        List<String> groupByFields = new ArrayList<>();
        if (entityMatchEnabled) {
            // In the future, Transaction could have more account fields, need
            // to consider:
            // 1. Are they needed in transaction store
            // 2. How to properly and efficiently retain them -- Keeping adding
            // in group fields could have performance concern; Add a join?
            groupByFields.add(InterfaceName.CustomerAccountId.name());
        }
        groupByFields.addAll(Arrays.asList( //
                InterfaceName.AccountId.name(), //
                InterfaceName.ProductId.name(), //
                InterfaceName.ProductType.name(), //
                InterfaceName.TransactionType.name(), //
                InterfaceName.PeriodId.name(), //
                InterfaceName.PeriodName.name()));
        config.setGroupByFields(groupByFields);
        step.setConfiguration(JsonUtils.serialize(config));
        step.setConfiguration(appendEngineConf(config, heavyMemoryEngineConfig()));
        return step;
    }

    private TransformationStepConfig collectPeriods(int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_COLLECTOR);
        step.setInputSteps(Collections.singletonList(inputStep));
        PeriodCollectorConfig config = new PeriodCollectorConfig();
        config.setPeriodField(InterfaceName.PeriodId.name());
        config.setPeriodNameField(InterfaceName.PeriodName.name());
        step.setConfiguration(appendEngineConf(config, heavyMemoryEngineConfig()));
        return step;
    }

    private TransformationStepConfig collectPeriodData(int periodsStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_FILTER);
        step.setInputSteps(Collections.singletonList(periodsStep));
        addBaseTables(step, dailyTable.getName());

        PeriodDataFilterConfig config = new PeriodDataFilterConfig();
        config.setPeriodField(InterfaceName.PeriodId.name());
        config.setPeriodNameField(InterfaceName.PeriodName.name());
        config.setEarliestTransactionDate(earliestTransaction);
        config.setPeriodStrategies(periodStrategies);
        config.setMultiPeriod(true);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig updatePeriodStore(List<Table> periodTables, int periodsStep, int periodAgrStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_DISTRIBUTOR);
        List<Integer> inputSteps = new ArrayList<>();
        inputSteps.add(periodsStep);
        inputSteps.add(periodAgrStep);
        step.setInputSteps(inputSteps);

        Map<String, Integer> transactionIdxes = new HashMap<>();
        for (int i = 0; i < periodTables.size(); i++) {
            Table periodTable = periodTables.get(i);
            addBaseTables(step, periodTable.getName());
            transactionIdxes.put(PeriodStrategyUtils.getPeriodStrategyNameFromPeriodTableName(periodTable.getName(),
                    TableRoleInCollection.ConsolidatedPeriodTransaction), i + 2);
        }

        PeriodDataDistributorConfig config = new PeriodDataDistributorConfig();
        config.setMultiPeriod(true);
        config.setPeriodField(InterfaceName.PeriodId.name());
        config.setPeriodNameField(InterfaceName.PeriodName.name());
        config.setTransactionIdxes(transactionIdxes);
        config.setRetryable(true);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

}
