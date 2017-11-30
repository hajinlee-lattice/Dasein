package com.latticeengines.cdl.workflow.steps;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.CEAttr;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_PROFILER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SORTER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_STATS_CALCULATOR;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_TRANSACTION_AGGREGATOR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.CalculateStatsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodCollectorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodConvertorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodDataAggregaterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodDataDistributorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProductMapperConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransactionAggregateConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.transaction.NamedPeriod;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.transaction.TransactionMetrics;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CalculatePurchaseHistoryConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.domain.exposed.util.TimeSeriesUtils;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;

@Component("calculatePurchaseHistory")
public class CalculatePurchaseHistory extends ProfileStepBase<CalculatePurchaseHistoryConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(CalculatePurchaseHistory.class);

    @Autowired
    YarnConfiguration yarnConfiguration;

    @Autowired
    DataFeedProxy dataFeedProxy;

    @Autowired
    DataCollectionProxy dataCollectionProxy;

    private static final String MASTER_TABLE_PREFIX = "PurchaseHistory";
    private static final String PROFILE_TABLE_PREFIX = "PurchaseHistoryProfile";
    private static final String STATS_TABLE_PREFIX = "PurchaseHistoryStats";
    private static final String SORTED_TABLE_PREFIX = "SortedDailyTransaction";
    private static final String SORTED_PERIOD_TABLE_PREFIX = "SortedPeriodTransaction";

    private DataFeed feed;

    private Table rawTable;
    private Table dailyTable;
    private Table periodTable;
    private Table accountTable;

    private CustomerSpace customerSpace;
    private Map<String, Product> productMap;

    private int productAgrStep;
    private int dailyAgrStep;
    private int dayPeriodStep;
    private int periodedStep;
    private int periodAgrStep;
    private int periodsStep;
    private static int aggregateStep;
    private static int profileStep;
    private static int bucketStep;

    private DataCollection.Version activeVersion;


    @Override
    protected BusinessEntity getEntity() {
        return BusinessEntity.PurchaseHistory;
    }

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        customerSpace = configuration.getCustomerSpace();

        feed = dataFeedProxy.getDataFeed(customerSpace.toString());

        Table productTable = dataCollectionProxy.getTable(customerSpace.toString(), TableRoleInCollection.ConsolidatedProduct);
        if (productTable == null) {
            throw new IllegalStateException("Cannot find the product table in default collection");
        }
        log.info(String.format("productTableName for customer %s is %s", configuration.getCustomerSpace().toString(),
                productTable.getName()));

        accountTable = dataCollectionProxy.getTable(customerSpace.toString(), TableRoleInCollection.ConsolidatedAccount);
        productMap = TimeSeriesUtils.loadProductMap(yarnConfiguration, productTable);

        activeVersion = dataCollectionProxy.getActiveVersion(customerSpace.toString());
        rawTable = dataCollectionProxy.getTable(customerSpace.toString(), TableRoleInCollection.ConsolidatedRawTransaction, null);
        dailyTable = dataCollectionProxy.getTable(customerSpace.toString(), TableRoleInCollection.ConsolidatedDailyTransaction, activeVersion);
        periodTable = dataCollectionProxy.getTable(customerSpace.toString(), TableRoleInCollection.ConsolidatedPeriodTransaction, activeVersion);

        if (feed.getRebuildTransaction()) {
            TimeSeriesUtils.cleanupPeriodData(yarnConfiguration, dailyTable.getExtracts().get(0).getPath());
            TimeSeriesUtils.cleanupPeriodData(yarnConfiguration, periodTable.getExtracts().get(0).getPath());
        }

        PipelineTransformationRequest request = generateRequest();
        return transformationProxy.getWorkflowConf(request, configuration.getPodId());
    }

    @Override
    protected void onPostTransformationCompleted() {
        String masterTableName = TableUtils.getFullTableName(MASTER_TABLE_PREFIX, pipelineVersion);
        String profileTableName = TableUtils.getFullTableName(PROFILE_TABLE_PREFIX, pipelineVersion);
        String statsTableName = TableUtils.getFullTableName(STATS_TABLE_PREFIX, pipelineVersion);
        String sortedTableName = TableUtils.getFullTableName(SORTED_TABLE_PREFIX, pipelineVersion);
        String sortedPeriodTableName = TableUtils.getFullTableName(SORTED_PERIOD_TABLE_PREFIX, pipelineVersion);

        updateEntityValueMapInContext(BusinessEntity.Transaction, TABLE_GOING_TO_REDSHIFT, sortedTableName, String.class);
        updateEntityValueMapInContext(BusinessEntity.Transaction, APPEND_TO_REDSHIFT_TABLE, false, Boolean.class);
        updateEntityValueMapInContext(BusinessEntity.PeriodTransaction, TABLE_GOING_TO_REDSHIFT, sortedPeriodTableName, String.class);
        updateEntityValueMapInContext(BusinessEntity.PeriodTransaction, APPEND_TO_REDSHIFT_TABLE, false, Boolean.class);

        upsertProfileTable(profileTableName, TableRoleInCollection.PurchaseHistoryProfile);
        upsertMasterTable(masterTableName);

        updateEntityValueMapInContext(SERVING_STORE_IN_STATS, masterTableName, String.class);
        updateEntityValueMapInContext(STATS_TABLE_NAMES, statsTableName, String.class);
        if (feed.getRebuildTransaction()) {
            dataFeedProxy.rebuildTransaction(customerSpace.toString(), Boolean.FALSE);
        }
    }

    private PipelineTransformationRequest generateRequest() {
        DataFeed feed = dataFeedProxy.getDataFeed(customerSpace.toString());
        try {
            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("CalculatePurchaseHistory");
            request.setSubmitter(customerSpace.getTenantId());
            request.setKeepTemp(false);
            request.setEnableSlack(false);
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            int startIdx = 0;
            if (feed.getRebuildTransaction()) {
                productAgrStep = 0;
                periodedStep = 1;
                dailyAgrStep = 2;
                dayPeriodStep = 3;
                periodAgrStep = 5;
                periodsStep = 6;
                TransformationStepConfig productAgr  = rollupProduct(productMap);
                TransformationStepConfig perioded = addPeriod();
                TransformationStepConfig dailyAgr  = aggregateDaily();
                TransformationStepConfig dayPeriods = collectDays();
                TransformationStepConfig updateDaily  = updateDailyStore();
                TransformationStepConfig periodAgr  = aggregatePeriods();
                TransformationStepConfig periods  = collectPeriods();
                TransformationStepConfig updatePeriod  = updatePeriodStore();
                steps.add(productAgr);
                steps.add(perioded);
                steps.add(dailyAgr);
                steps.add(dayPeriods);
                steps.add(updateDaily);
                steps.add(periodAgr);
                steps.add(periods);
                steps.add(updatePeriod);

                startIdx = steps.size();
            }

            aggregateStep = startIdx + 2;
            profileStep = startIdx + 3;
            bucketStep = startIdx + 4;
            // -----------
            TransformationStepConfig sortDaily = sort(customerSpace, dailyTable, SORTED_TABLE_PREFIX);
            TransformationStepConfig sortPeriod = sort(customerSpace, periodTable, SORTED_PERIOD_TABLE_PREFIX);
            TransformationStepConfig aggregate = aggregate(customerSpace, MASTER_TABLE_PREFIX, dailyTable.getName(),
                    accountTable.getName(), productMap);
            TransformationStepConfig profile = profile();
            TransformationStepConfig bucket = bucket();
            TransformationStepConfig calc = calcStats(customerSpace, STATS_TABLE_PREFIX);
            TransformationStepConfig sortProfile = sortProfile(customerSpace, PROFILE_TABLE_PREFIX);
            steps.add(sortDaily);
            steps.add(sortPeriod);
            steps.add(aggregate);
            steps.add(profile);
            steps.add(bucket);
            steps.add(calc);
            steps.add(sortProfile);
            request.setSteps(steps);
            return request;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private TransformationStepConfig rollupProduct(Map<String, Product> productMap) {
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PRODUCT_MAPPER);
        String tableSourceName = "CustomerUniverse";
        String sourceTableName = rawTable.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step2.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step2.setBaseTables(baseTables);

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


    private TransformationStepConfig aggregateDaily() {
        System.out.print("Aggr daily perioded step " + periodedStep);
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PERIOD_DATA_AGGREGATER);
        step2.setInputSteps(Collections.singletonList(periodedStep));
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

    private TransformationStepConfig collectDays() {
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTransformer(DataCloudConstants.PERIOD_COLLECTOR);
        step2.setInputSteps(Collections.singletonList(dailyAgrStep));
        PeriodCollectorConfig config = new PeriodCollectorConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());
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
        step2.setInputSteps(Collections.singletonList(dailyAgrStep));
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
        step2.setInputSteps(Collections.singletonList(periodAgrStep));
        PeriodCollectorConfig config = new PeriodCollectorConfig();
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

    private TransformationStepConfig sort(CustomerSpace customerSpace, Table table, String prefix) {
        TransformationStepConfig step = new TransformationStepConfig();
        String tableSourceName = "CustomerUniverse";
        SourceTable sourceTable = new SourceTable(table.getName(), customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);
        step.setTransformer(TRANSFORMER_SORTER);

        SorterConfig config = new SorterConfig();
        config.setPartitions(20);
        String sortingKey = TableRoleInCollection.SortedProduct.getForeignKeysAsStringList().get(0);
        config.setSortingField(sortingKey);
        config.setCompressResult(true);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(prefix);
        step.setTargetTable(targetTable);

        return step;
    }

    private TransformationStepConfig aggregate(CustomerSpace customerSpace, String masterTablePrefix,
            String transactionTableName, String accountTableName, Map<String, Product> productMap) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_TRANSACTION_AGGREGATOR);

        String transactionSourceName = "Transaction";
        String accountSourceName = "Account";
        SourceTable transactionTable = new SourceTable(transactionTableName, customerSpace);
        SourceTable accountTable = new SourceTable(accountTableName, customerSpace);
        List<String> baseSources = new ArrayList<String>();
        baseSources.add(transactionSourceName);
        baseSources.add(accountSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(transactionSourceName, transactionTable);
        baseTables.put(accountSourceName, accountTable);
        step.setBaseTables(baseTables);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(masterTablePrefix);
        step.setTargetTable(targetTable);

        TransactionAggregateConfig conf = new TransactionAggregateConfig();
        conf.setProductMap(productMap);
        conf.setTransactionType("Purchase");
        conf.setIdField(InterfaceName.AccountId.name());
        conf.setAccountField(InterfaceName.AccountId.name());
        conf.setProductField(InterfaceName.ProductId.name());
        conf.setDateField(InterfaceName.TransactionDate.name());
        conf.setTypeField(InterfaceName.TransactionType.name());
        conf.setQuantityField(InterfaceName.TotalQuantity.name());
        conf.setAmountField(InterfaceName.TotalAmount.name());

        List<String> periods = new ArrayList<>();
        List<String> metrics = new ArrayList<>();

        periods.add(NamedPeriod.HASEVER.getName());
        metrics.add(TransactionMetrics.PURCHASED.getName());
        // TODO: do not support quarter in M16
//        periods.add(NamedPeriod.LASTQUARTER.getName());
//        metrics.add(TransactionMetrics.QUANTITY.getName());
//        periods.add(NamedPeriod.LASTQUARTER.getName());
//        metrics.add(TransactionMetrics.AMOUNT.getName());
        conf.setPeriods(periods);
        conf.setMetrics(metrics);

        String confStr = appendEngineConf(conf, lightEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig profile() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(aggregateStep));
        step.setTransformer(TRANSFORMER_PROFILER);
        ProfileConfig conf = new ProfileConfig();
        conf.setEncAttrPrefix(CEAttr);
        String confStr = appendEngineConf(conf, heavyEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig bucket() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(aggregateStep, profileStep));
        step.setTransformer(TRANSFORMER_BUCKETER);
        step.setConfiguration(emptyStepConfig(heavyEngineConfig()));
        return step;
    }

    private TransformationStepConfig calcStats(CustomerSpace customerSpace, String statsTablePrefix) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(bucketStep, profileStep));
        step.setTransformer(TRANSFORMER_STATS_CALCULATOR);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(statsTablePrefix);
        step.setTargetTable(targetTable);

        CalculateStatsConfig conf = new CalculateStatsConfig();
        step.setConfiguration(appendEngineConf(conf, heavyEngineConfig()));
        return step;
    }

    private TransformationStepConfig sortProfile(CustomerSpace customerSpace, String profileTablePrefix) {
        TransformationStepConfig step = new TransformationStepConfig();
        List<Integer> inputSteps = Collections.singletonList(profileStep);
        step.setInputSteps(inputSteps);
        step.setTransformer(TRANSFORMER_SORTER);

        SorterConfig conf = new SorterConfig();
        conf.setPartitions(1);
        conf.setCompressResult(true);
        conf.setSortingField(DataCloudConstants.PROFILE_ATTR_ATTRNAME);
        String confStr = appendEngineConf(conf, lightEngineConfig());
        step.setConfiguration(confStr);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(profileTablePrefix);
        step.setTargetTable(targetTable);

        return step;
    }

    private void upsertMasterTable(String masterTableName) {
        String customerSpace = configuration.getCustomerSpace().toString();

        Table masterTable = metadataProxy.getTable(customerSpace, masterTableName);
        if (masterTable == null) {
            throw new RuntimeException("Failed to find master table in customer " + customerSpace);
        } else {
            updateTable(customerSpace, masterTableName, masterTable);
        }
        DataCollection.Version inactiveVersion = dataCollectionProxy.getInactiveVersion(customerSpace);

        dataCollectionProxy.upsertTable(customerSpace, masterTableName, TableRoleInCollection.CalculatedPurchaseHistory,
                inactiveVersion);
        masterTable = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.CalculatedPurchaseHistory,
                inactiveVersion);
        if (masterTable == null) {
            throw new IllegalStateException("Cannot find the upserted master table in data collection.");
        }
    }

    private void updateTable(String customerSpace, String masterTableName, Table masterTable) {
        List<Attribute> attributes = masterTable.getAttributes();

        for (Attribute attribute : attributes) {
            attribute.setCategory(Category.PRODUCT_SPEND.getName());

            if (!InterfaceName.AccountId.name().equalsIgnoreCase(attribute.getName())) {
                String productId = TransactionMetrics.getProductIdFromAttr(attribute.getName());
                if (StringUtils.isBlank(productId)) {
                    throw new RuntimeException("Cannot parse product id from attribute name " + attribute.getName());
                }
                String productName = null;
                Product product = productMap.get(productId);
                if (product != null) {
                    productName = product.getProductName();
                }
                if (StringUtils.isBlank(productName)) {
                    System.out.println(JsonUtils.pprint(productMap));
                    throw new IllegalArgumentException("Cannot find product name for product id " + productId);
                } else {
                    String periodName = TransactionMetrics.getPeriodFromAttr(attribute.getName());
                    NamedPeriod period = NamedPeriod.fromName(periodName);
                    String metricName = TransactionMetrics.getMetricFromAttr(attribute.getName());
                    TransactionMetrics metric = TransactionMetrics.fromName(metricName);
                    attribute.setDisplayName(getDisplayName(period, metric));
                }
                attribute.setSubcategory(productName);
            }

            attribute.removeAllowedDisplayNames();
        }

        metadataProxy.updateTable(customerSpace, masterTableName, masterTable);
    }

    private String getDisplayName(NamedPeriod period, TransactionMetrics metric) {
        switch (metric) {
        case PURCHASED:
            return purchasedAttrName(period);
        case AMOUNT:
            return amountAttrName(period);
        case QUANTITY:
            return quantityAttrName(period);
        default:
            throw new UnsupportedOperationException("Transaction metric " + metric + " is not supported for now.");
        }
    }

    private String purchasedAttrName(NamedPeriod period) {
        switch (period) {
        case HASEVER:
            return "Has Purchased";
        default:
            throw new UnsupportedOperationException("Transaction metric " + TransactionMetrics.PURCHASED
                    + " does not support period " + period + " for now.");
        }
    }

    private String amountAttrName(NamedPeriod period) {
        switch (period) {
            case HASEVER:
                return "Total Spend";
            case LASTQUARTER:
                return "Last Quarter Spend";
            default:
                throw new UnsupportedOperationException("Transaction metric " + TransactionMetrics.AMOUNT
                        + " does not support period " + period + " for now.");
        }
    }

    private String quantityAttrName(NamedPeriod period) {
        switch (period) {
            case HASEVER:
                return "Total Purchased";
            case LASTQUARTER:
                return "Last Quarter Purchased";
            default:
                throw new UnsupportedOperationException("Transaction metric " + TransactionMetrics.QUANTITY
                        + " does not support period " + period + " for now.");
        }
    }

}
