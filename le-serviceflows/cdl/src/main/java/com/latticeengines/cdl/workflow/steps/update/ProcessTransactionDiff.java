package com.latticeengines.cdl.workflow.steps.update;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_CONSOLIDATE_RETAIN;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
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
import com.latticeengines.domain.exposed.util.TimeSeriesUtils;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;

@Component(ProcessTransactionDiff.BEAN_NAME)
public class ProcessTransactionDiff extends BaseProcessDiffStep<ProcessTransactionStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ProcessTransactionDiff.class);

    static final String BEAN_NAME = "processTransactionDiff";

    private Map<String, Product> productMap;
    private int dailyRawStep, productAgrStep, addPeriodStep;
    private int dailyAgrStep, periodsStep, periodDataStep, periodAgrStep;

    private Table rawTable, dailyTable, periodTable;
    private String dailyTablePrefix, periodTablePrefix, servingStorePrimaryKey;
    private String earliestTransaction;
    private String diffTableName;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();

        dailyTablePrefix = TableRoleInCollection.AggregatedTransaction.name();
        periodTablePrefix = TableRoleInCollection.AggregatedPeriodTransaction.name();
        servingStorePrimaryKey = InterfaceName.__Composite_Key__.name();

        rawTable = dataCollectionProxy.getTable(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedRawTransaction, inactive);
        dailyTable = dataCollectionProxy.getTable(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedDailyTransaction, inactive);
        periodTable = dataCollectionProxy.getTable(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedPeriodTransaction, inactive);

        Map<BusinessEntity, String> diffTableNames = getMapObjectFromContext(ENTITY_DIFF_TABLES,
                BusinessEntity.class, String.class);
        diffTableName = diffTableNames.get(BusinessEntity.Transaction);

        DataFeed feed = dataFeedProxy.getDataFeed(customerSpace.toString());
        earliestTransaction = DateTimeUtils.dayPeriodToDate(feed.getEarliestTransaction());

        loadProductMap();
    }

    @Override
    protected void onPostTransformationCompleted() {
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
        periodsStep = 7;
        periodDataStep = 8;
        periodAgrStep = 9;

        TransformationStepConfig dailyRaw = collectDailyData();
        TransformationStepConfig productAgr  = rollupProduct(productMap);
        TransformationStepConfig periodAdded = addPeriod();
        TransformationStepConfig dailyAgr  = aggregateDaily();
        TransformationStepConfig dailyRetained = retainFields(dailyAgrStep, TableRoleInCollection.AggregatedTransaction);
        TransformationStepConfig cleanDaily  = cleanupDailyHistory();
        TransformationStepConfig updateDaily  = updateDailyStore();
        TransformationStepConfig periods  = collectPeriods();
        TransformationStepConfig periodData = collectPeriodData();
        TransformationStepConfig periodAgr  = aggregatePeriods();
        TransformationStepConfig periodRetained  = retainFields(periodAgrStep, TableRoleInCollection.AggregatedPeriodTransaction);
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
        productMap = TimeSeriesUtils.loadProductMap(yarnConfiguration, productTable);
    }

    private TransformationStepConfig collectDailyData() {
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PERIOD_DATA_FILTER);

        String tableSourceName = "DailyRaw";
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
        config.setPeriodStrategy(PeriodStrategy.CalendarMonth);
        config.setPeriodField(InterfaceName.PeriodId.name());
        step2.setConfiguration(JsonUtils.serialize(config));
        return step2;
    }

    private TransformationStepConfig cleanupDailyHistory() {
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PERIOD_DATA_CLEANER);

        String sourceName1 = "DailyPeriodTable";
        SourceTable sourceTable1 = new SourceTable(diffTableName, customerSpace);
        String sourceName2 = "DailyTable";
        SourceTable sourceTable2 = new SourceTable(dailyTable.getName(), customerSpace);
        List<String> baseSources = Arrays.asList(sourceName1, sourceName2);
        step2.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(sourceName1, sourceTable1);
        baseTables.put(sourceName2, sourceTable2);

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
        step2.setInputSteps(Collections.singletonList(addPeriodStep));
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(dailyTablePrefix);
        targetTable.setPrimaryKey(servingStorePrimaryKey);
        targetTable.setExpandBucketedAttrs(false);
        step2.setTargetTable(targetTable);
        PeriodDataAggregaterConfig config = new PeriodDataAggregaterConfig();
        config.setSumFields(Collections.singletonList("Amount"));
        config.setSumOutputFields(Collections.singletonList("TotalAmount"));
        config.setSumLongFields(Collections.singletonList("Quantity"));
        config.setSumLongOutputFields(Collections.singletonList("TotalQuantity"));
        config.setGoupByFields(Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.ContactId.name(),
                InterfaceName.ProductId.name(), InterfaceName.TransactionType.name(),
                InterfaceName.TransactionDate.name(),
                InterfaceName.PeriodId.name(),
                InterfaceName.TransactionDayPeriod.name()));
        step2.setConfiguration(JsonUtils.serialize(config));
        return step2;
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
        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private TransformationStepConfig aggregatePeriods() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_AGGREGATER);
        step.setInputSteps(Collections.singletonList(periodDataStep));
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(periodTablePrefix);
        targetTable.setPrimaryKey(servingStorePrimaryKey);
        targetTable.setExpandBucketedAttrs(false);
        step.setTargetTable(targetTable);
        PeriodDataAggregaterConfig config = new PeriodDataAggregaterConfig();
        config.setSumFields(Collections.singletonList("TotalAmount"));
        config.setSumOutputFields(Collections.singletonList("TotalAmount"));
        config.setSumLongFields(Collections.singletonList("TotalQuantity"));
        config.setSumLongOutputFields(Collections.singletonList("TotalQuantity"));
        config.setGoupByFields(Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.ContactId.name(),
                InterfaceName.ProductId.name(), InterfaceName.TransactionType.name(),
                InterfaceName.PeriodId.name()));
        step.setConfiguration(JsonUtils.serialize(config));
        return step;
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

        String tableSourceName = "DailyTable";
        String sourceTableName = dailyTable.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step2.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step2.setBaseTables(baseTables);

        PeriodDataFilterConfig config = new PeriodDataFilterConfig();
        config.setPeriodField(InterfaceName.PeriodId.name());
        config.setPeriodStrategy(PeriodStrategy.CalendarMonth);
        config.setEarliestTransactionDate(earliestTransaction);
        step2.setConfiguration(JsonUtils.serialize(config));
        return step2;
    }

    private TransformationStepConfig cleanupPeriodHistory() {
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PERIOD_DATA_CLEANER);
        step2.setInputSteps(Collections.singletonList(periodsStep));

        String tableSourceName = "PeriodTable";
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

        String tableSourceName = "PeriodTable";
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

}
