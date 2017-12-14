package com.latticeengines.cdl.workflow.steps.rebuild;

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

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.CalculateStatsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransactionAggregateConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.transaction.NamedPeriod;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.transaction.TransactionMetrics;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.util.TimeSeriesUtils;

@Component(ProfilePurchaseHistory.BEAN_NAME)
public class ProfilePurchaseHistory extends BaseSingleEntityProfileStep<ProcessTransactionStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ProfilePurchaseHistory.class);

    static final String BEAN_NAME = "profilePurchaseHistory";

    private int aggregateStep, profileStep, bucketStep;
    private Map<String, Product> productMap;
    private String dailyTableName;
    private String accountTableName;

    @Inject
    private Configuration yarnConfiguration;

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

        aggregateStep = 0;
        profileStep = 1;
        bucketStep = 2;

        TransformationStepConfig aggregate = aggregate(customerSpace, dailyTableName, accountTableName, productMap);
        TransformationStepConfig profile = profile();
        TransformationStepConfig bucket = bucket();
        TransformationStepConfig calc = calcStats(customerSpace, statsTablePrefix);
        TransformationStepConfig sortProfile = sortProfile(customerSpace, profileTablePrefix);
        steps.add(aggregate);
        steps.add(profile);
        steps.add(bucket);
        steps.add(calc);
        steps.add(sortProfile);

        // -----------
        request.setSteps(steps);
        return request;
    }

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        loadProductMap();

        dailyTableName = dataCollectionProxy.getTableName(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedDailyTransaction, inactive);
        if (StringUtils.isBlank(dailyTableName)) {
            throw new IllegalStateException("Cannot find daily table.");
        }

        accountTableName = getAccountTableName();
        if (StringUtils.isBlank(accountTableName)) {
            throw new IllegalStateException("Cannot find account master table.");
        }

        // TODO: not ready to publish purchase history to redshift
        publishToRedshift = false;
    }

    private String getAccountTableName() {
        String accountTableName =  dataCollectionProxy.getTableName(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedAccount, inactive);
        if (StringUtils.isBlank(accountTableName)) {
            // might because the merge account step was skipped
            accountTableName =  dataCollectionProxy.getTableName(customerSpace.toString(),
                    TableRoleInCollection.ConsolidatedAccount, active);
            if (StringUtils.isNotBlank(accountTableName)) {
                log.info("Found account batch store in active version " + active);
            }
        } else {
            log.info("Found account batch store in inactive version " + inactive);
        }
        return accountTableName;
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

    private TransformationStepConfig aggregate(CustomerSpace customerSpace, String transactionTableName, //
                                               String accountTableName, Map<String, Product> productMap) {
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
        String confStr = appendEngineConf(conf, lightEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig bucket() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(aggregateStep, profileStep));
        step.setTransformer(TRANSFORMER_BUCKETER);
        step.setConfiguration(emptyStepConfig(lightEngineConfig()));
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
        step.setConfiguration(appendEngineConf(conf, lightEngineConfig()));
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

    @Override
    protected void enrichTableSchema(Table servingStoreTable) {
        List<Attribute> attributes = servingStoreTable.getAttributes();

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
