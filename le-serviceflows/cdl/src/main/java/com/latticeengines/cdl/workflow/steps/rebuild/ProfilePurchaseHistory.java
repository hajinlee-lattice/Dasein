package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.CEAttr;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_PROFILER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SORTER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_STATS_CALCULATOR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ActivityMetricsCuratorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ActivityMetricsPivotConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.CalculateStatsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.util.ActivityMetricsUtils;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.domain.exposed.util.TimeSeriesUtils;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.cdl.ActivityMetricsProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;

@Component(ProfilePurchaseHistory.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProfilePurchaseHistory extends BaseSingleEntityProfileStep<ProcessTransactionStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ProfilePurchaseHistory.class);

    static final String BEAN_NAME = "profilePurchaseHistory";

    private List<Integer> initSteps;
    private int curateStep, pivotStep, profileStep, bucketStep;
    private Map<String, List<Product>> productMap;
    private String dailyTableName;
    private String accountTableName;
    private String productTableName;
    private List<Table> periodTables;
    private List<PeriodStrategy> periodStrategies;
    private List<ActivityMetrics> purchaseMetrics;
    private String maxTxnDate;

    private String curatedMetricsTablePrefix;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private PeriodProxy periodProxy;

    @Inject
    private ActivityMetricsProxy metricsProxy;

    @Override
    protected BusinessEntity getEntityToBeProfiled() {
        return BusinessEntity.PurchaseHistory;
    }

    @Override
    protected TableRoleInCollection profileTableRole() {
        return TableRoleInCollection.PurchaseHistoryProfile;
    }

    @Override
    protected PipelineTransformationRequest getTransformRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("CalculatePurchaseHistory");
        request.setSubmitter(customerSpace.getTenantId());
        request.setKeepTemp(false);
        request.setEnableSlack(false);
        // -----------
        List<TransformationStepConfig> steps = new ArrayList<>();

        // TODO: Should have more abstract way to support such multi-period tables
        initSteps = new ArrayList<>();
        curateStep = periodTables.size();
        pivotStep = periodTables.size() + 1;
        profileStep = periodTables.size() + 2;
        bucketStep = periodTables.size() + 3;

        for (int i = 0; i < periodTables.size(); i++) {
            TransformationStepConfig init = init(periodTables.get(i));
            steps.add(init);
            initSteps.add(i);
        }

        TransformationStepConfig curate = curate();
        TransformationStepConfig pivot = pivot();
        TransformationStepConfig profile = profile();
        TransformationStepConfig bucket = bucket();
        TransformationStepConfig calc = calcStats();
        TransformationStepConfig sortProfile = sortProfile();
        steps.add(curate);
        steps.add(pivot);
        steps.add(profile);
        steps.add(bucket);
        steps.add(calc);
        steps.add(sortProfile);

        // -----------
        request.setSteps(steps);
        return request;
    }

    @Override
    protected void onPostTransformationCompleted() {
        String curatedMetricsTableName = TableUtils.getFullTableName(curatedMetricsTablePrefix, pipelineVersion);
        if (metadataProxy.getTable(customerSpace.toString(), curatedMetricsTableName) == null) {
            throw new IllegalStateException("Cannot find result curated metrics table");
        }
        updateEntityValueMapInContext(BusinessEntity.DepivotedPurchaseHistory, TABLE_GOING_TO_REDSHIFT,
                curatedMetricsTableName,
                String.class);
        updateEntityValueMapInContext(BusinessEntity.DepivotedPurchaseHistory, APPEND_TO_REDSHIFT_TABLE, true,
                Boolean.class);
        super.onPostTransformationCompleted();
        generateReport();
    }

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        loadProductMap();

        dailyTableName = getDailyTableName();
        if (StringUtils.isBlank(dailyTableName)) {
            throw new IllegalStateException("Cannot find daily table.");
        }

        accountTableName = getAccountTableName();
        if (StringUtils.isBlank(accountTableName)) {
            throw new IllegalStateException("Cannot find account master table.");
        }

        maxTxnDate = findLatestTransactionDate();

        periodStrategies = periodProxy.getPeriodStrategies(customerSpace.toString());
        periodTables = dataCollectionProxy.getTables(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedPeriodTransaction, inactive);

        purchaseMetrics = metricsProxy.getActivityMetrics(customerSpace.toString(), ActivityType.PurchaseHistory);
        // HasPurchased is the default metrics to calculate
        purchaseMetrics.add(createHasPurchasedMetrics());

        curatedMetricsTablePrefix = TableRoleInCollection.CalculatedDepivotedPurchaseHistory.name();
    }

    private String getDailyTableName() {
        String dailyTableName = dataCollectionProxy.getTableName(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedDailyTransaction, inactive);
        if (StringUtils.isBlank(dailyTableName)) {
            dailyTableName = dataCollectionProxy.getTableName(customerSpace.toString(),
                    TableRoleInCollection.ConsolidatedDailyTransaction, active);
            if (StringUtils.isNotBlank(dailyTableName)) {
                log.info("Found daily table in active version " + active);
            }
        } else {
            log.info("Found daily table in inactive version " + inactive);
        }
        return dailyTableName;
    }

    private String getAccountTableName() {
        String accountTableName = dataCollectionProxy.getTableName(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedAccount, inactive);
        if (StringUtils.isBlank(accountTableName)) {
            accountTableName = dataCollectionProxy.getTableName(customerSpace.toString(),
                    TableRoleInCollection.ConsolidatedAccount, active);
            if (StringUtils.isNotBlank(accountTableName)) {
                log.info("Found account batch store in active version " + active);
            }
        } else {
            log.info("Found account batch store in inactive version " + inactive);
        }
        return accountTableName;
    }

    private String findLatestTransactionDate() {
        DataFeed feed = dataFeedProxy.getDataFeed(customerSpace.toString());
        if (feed.getLatestTransaction() == null) {
            throw new IllegalStateException("Latest transaction day period in data feed is empty");
        }
        return DateTimeUtils.dayPeriodToDate(feed.getLatestTransaction());
    }

    private void loadProductMap() {
        Table productTable = dataCollectionProxy.getTable(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedProduct, inactive);
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
        productTableName = productTable.getName();
        productMap = TimeSeriesUtils.loadProductMap(yarnConfiguration, productTable);
        // TODO: Wait for @Ke's change, then remove commented part
        //productMap.values().removeIf(products -> products.get(0).getProductType() != ProductType.ANALYTIC);
    }

    private TransformationStepConfig init(Table periodTable) {
        TransformationStepConfig step = new TransformationStepConfig();
        List<String> baseSources = Arrays.asList(periodTable.getName(), accountTableName, productTableName);
        step.setBaseSources(baseSources);
        SourceTable periodSourceTable = new SourceTable(periodTable.getName(), customerSpace);
        SourceTable accountSourceTable = new SourceTable(accountTableName, customerSpace);
        SourceTable productSourceTable = new SourceTable(productTableName, customerSpace);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(periodTable.getName(), periodSourceTable);
        baseTables.put(productTableName, productSourceTable);
        baseTables.put(accountTableName, accountSourceTable);
        step.setBaseTables(baseTables);
        step.setTransformer(DataCloudConstants.PURCHASE_METRICS_INITIATOR);
        step.setConfiguration("{}");
        return step;
    }

    private TransformationStepConfig curate() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(initSteps);
        step.setTransformer(DataCloudConstants.ACTIVITY_METRICS_CURATOR);
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(curatedMetricsTablePrefix);
        targetTable.setPrimaryKey(InterfaceName.__Composite_Key__.name());
        step.setTargetTable(targetTable);

        ActivityMetricsCuratorConfig conf = new ActivityMetricsCuratorConfig();
        conf.setGroupByFields(Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.ProductId.name()));
        conf.setCurrentDate(maxTxnDate);
        conf.setMetrics(purchaseMetrics);
        conf.setPeriodStrategies(periodStrategies);

        step.setConfiguration(JsonUtils.serialize(conf));
        return step;
    }

    private TransformationStepConfig pivot() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(curateStep));
        step.setTransformer(DataCloudConstants.ACTIVITY_METRICS_PIVOT);
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(servingStoreTablePrefix);
        step.setTargetTable(targetTable);

        ActivityMetricsPivotConfig conf = new ActivityMetricsPivotConfig();
        conf.setActivityType(ActivityType.PurchaseHistory);
        conf.setGroupByField(InterfaceName.AccountId.name());
        conf.setPivotField(InterfaceName.ProductId.name());
        conf.setProductMap(productMap);

        step.setConfiguration(JsonUtils.serialize(conf));
        return step;
    }

    private TransformationStepConfig profile() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(pivotStep));
        step.setTransformer(TRANSFORMER_PROFILER);
        ProfileConfig conf = new ProfileConfig();
        conf.setEncAttrPrefix(CEAttr);
        String confStr = appendEngineConf(conf, lightEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig bucket() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(pivotStep, profileStep));
        step.setTransformer(TRANSFORMER_BUCKETER);
        step.setConfiguration(emptyStepConfig(lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig calcStats() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(bucketStep, profileStep));
        step.setTransformer(TRANSFORMER_STATS_CALCULATOR);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(statsTablePrefix);
        step.setTargetTable(targetTable);

        CalculateStatsConfig conf = new CalculateStatsConfig();
        step.setConfiguration(appendEngineConf(conf, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig sortProfile() {
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

    @Override
    protected void enrichTableSchema(Table servingStoreTable) {
        List<Attribute> attributes = servingStoreTable.getAttributes();

        for (Attribute attribute : attributes) {
            attribute.setCategory(Category.PRODUCT_SPEND.getName());

            if (!InterfaceName.AccountId.name().equalsIgnoreCase(attribute.getName())) {
                String productId = ActivityMetricsUtils.getActivityIdFromFullName(attribute.getName());
                if (StringUtils.isBlank(productId)) {
                    throw new RuntimeException("Cannot parse product id from attribute name " + attribute.getName());
                }

                String productName = null;
                List<Product> products = productMap.get(productId);
                if (products != null) {
                    for (Product product : products) {
                        productName = product.getProductName();
                        if (productName != null) {
                            break;
                        }
                    }
                }
                if (productName == null) {
                    productName = productId;
                }
                if (StringUtils.isBlank(productName)) {
                    throw new IllegalArgumentException("Cannot find product name for product id " + productId
                            + " in product map " + JsonUtils.serialize(productMap));
                }

                Pair<String, String> displayNames = ActivityMetricsUtils
                        .getDisplayNamesFromFullName(attribute.getName(), maxTxnDate, periodStrategies);
                attribute.setDisplayName(displayNames.getLeft());
                attribute.setSecondaryDisplayName(displayNames.getRight());
                attribute.setSubcategory(productName);
            }
            attribute.removeAllowedDisplayNames();
        }
    }

    private ActivityMetrics createHasPurchasedMetrics() {
        Tenant tenant = MultiTenantContext.getTenant();
        ActivityMetrics metrics = new ActivityMetrics();
        metrics.setMetrics(InterfaceName.HasPurchased);
        metrics.setPeriodsConfig(Arrays.asList(TimeFilter.ever()));
        metrics.setType(ActivityType.PurchaseHistory);
        metrics.setTenant(tenant);
        metrics.setEOL(false);
        metrics.setDeprecated(null);
        metrics.setCreated(new Date());
        metrics.setUpdated(metrics.getCreated());
        return metrics;
    }

    private void generateReport() {
        ObjectNode report = getObjectFromContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(),
                ObjectNode.class);
        updateReportPayload(report);
        putObjectInContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(), report);
    }

    private void updateReportPayload(ObjectNode report) {
        try {
            JsonNode entitiesSummaryNode = report.get(ReportPurpose.ENTITIES_SUMMARY.getKey());
            if (entitiesSummaryNode == null) {
                entitiesSummaryNode = report.putObject(ReportPurpose.ENTITIES_SUMMARY.getKey());
            }
            JsonNode entityNode = entitiesSummaryNode.get(entity.name());
            if (entityNode == null) {
                entityNode = ((ObjectNode) entitiesSummaryNode).putObject(entity.name());
            }
            JsonNode consolidateSummaryNode = entityNode.get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey());
            if (consolidateSummaryNode == null) {
                consolidateSummaryNode = ((ObjectNode) entityNode)
                        .putObject(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey());
            }
            ((ObjectNode) consolidateSummaryNode).put("PRODUCT", productMap.size());
            ((ObjectNode) consolidateSummaryNode).put("METRICS", purchaseMetrics.size());
        } catch (Exception e) {
            throw new RuntimeException("Fail to update report payload", e);
        }
    }


    /* Show respect to Yunfeng's code. R.I.P
    private TransformationStepConfig aggregate(CustomerSpace customerSpace, String transactionTableName, //
            String accountTableName, Map<String, List<Product>> productMap) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_TRANSACTION_AGGREGATOR);

        String transactionSourceName = "Transaction";
        String accountSourceName = "Account";
        SourceTable transactionTable = new SourceTable(transactionTableName, customerSpace);
        SourceTable accountTable = new SourceTable(accountTableName, customerSpace);
        List<String> baseSources = new ArrayList<>();
        baseSources.add(transactionSourceName);
        baseSources.add(accountSourceName);
        step.setBaseSources(baseSources);

        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(transactionSourceName, transactionTable);
        baseTables.put(accountSourceName, accountTable);
        step.setBaseTables(baseTables);

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
        // periods.add(NamedPeriod.LASTQUARTER.getName());
        // metrics.add(TransactionMetrics.QUANTITY.getName());
        // periods.add(NamedPeriod.LASTQUARTER.getName());
        // metrics.add(TransactionMetrics.AMOUNT.getName());
        conf.setPeriods(periods);
        conf.setMetrics(metrics);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(servingStoreTablePrefix);
        step.setTargetTable(targetTable);
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
     */
}
