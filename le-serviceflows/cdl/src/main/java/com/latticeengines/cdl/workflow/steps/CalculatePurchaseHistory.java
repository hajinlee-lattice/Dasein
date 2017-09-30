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

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
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
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.transaction.NamedPeriod;
import com.latticeengines.domain.exposed.metadata.transaction.TransactionMetrics;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CalculatePurchaseHistoryConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.datacloudapi.TransformationProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

@Component("calculatePurchaseHistory")
public class CalculatePurchaseHistory extends BaseTransformWrapperStep<CalculatePurchaseHistoryConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(CalculatePurchaseHistory.class);

    private static final String MASTER_TABLE_PREFIX = "PurchaseHistory";
    private static final String PROFILE_TABLE_PREFIX = "PurchaseHistoryProfile";
    private static final String STATS_TABLE_PREFIX = "PurchaseHistoryStats";

    private static int aggregateStep;
    private static int profileStep;
    private static int bucketStep;

    private Map<String, String> productMap;

    @Autowired
    private TransformationProxy transformationProxy;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private TransactionTableBuilder transactionTableBuilder;

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        String customerSpace = configuration.getCustomerSpace().toString();
        Table aggregatedTable = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.AggregatedTransaction);
        Table accountTable = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedAccount);
        Table productTable = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedProduct);
        if (aggregatedTable == null) {
            throw new IllegalStateException("Cannot find the transaction table in default collection");
        }
        if (productTable == null) {
            throw new IllegalStateException("Cannot find the product table in default collection");
        }
        log.info(String.format("transactionTableName for customer %s is %s", configuration.getCustomerSpace().toString(),
                aggregatedTable.getName()));
        log.info(String.format("productTableName for customer %s is %s", configuration.getCustomerSpace().toString(),
                productTable.getName()));

        productMap = loadProduct(productTable);

        // Table transactionTable = transactionTableBuilder.setupMasterTable("TransactionMaster", pipelineVersion, aggregatedTable);
        // metadataProxy.createTable(customerSpace, transactionTable.getName(), transactionTable);
        // PipelineTransformationRequest request = generateRequest(configuration.getCustomerSpace(), transactionTable, accountTable, productMap);

        PipelineTransformationRequest request = generateRequest(configuration.getCustomerSpace(), aggregatedTable, accountTable, productMap);
        return transformationProxy.getWorkflowConf(request, configuration.getPodId());
    }

    @Override
    protected void onPostTransformationCompleted() {
        String masterTableName = TableUtils.getFullTableName(MASTER_TABLE_PREFIX, pipelineVersion);
        String profileTableName = TableUtils.getFullTableName(PROFILE_TABLE_PREFIX, pipelineVersion);
        String statsTableName = TableUtils.getFullTableName(STATS_TABLE_PREFIX, pipelineVersion);
        Map<BusinessEntity, String> statsTableNameMap = getMapObjectFromContext(STATS_TABLE_NAMES, BusinessEntity.class, String.class);
        if (statsTableNameMap == null) {
            statsTableNameMap = new HashMap<BusinessEntity, String>();
            putObjectInContext(STATS_TABLE_NAMES, statsTableNameMap);
        }
        statsTableNameMap.put(BusinessEntity.PurchaseHistory, statsTableName);
        upsertTables(configuration.getCustomerSpace().toString(), masterTableName, profileTableName);
    }

    private PipelineTransformationRequest generateRequest(CustomerSpace customerSpace, Table transactionTable, Table accountTable,
                                                          Map<String, String> productMap) {
        String transactionTableName = transactionTable.getName();
        String accountTableName = accountTable.getName();
        try {
            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("CalculatePurchaseHistory");
            request.setSubmitter(customerSpace.getTenantId());
            request.setKeepTemp(false);
            request.setEnableSlack(false);
            aggregateStep = 0;
            profileStep = 1;
            bucketStep = 2;
            // -----------
            TransformationStepConfig aggregate = aggregate(customerSpace, MASTER_TABLE_PREFIX, transactionTableName, accountTableName, productMap);
            TransformationStepConfig profile = profile();
            TransformationStepConfig bucket = bucket();
            TransformationStepConfig calc = calcStats(customerSpace, STATS_TABLE_PREFIX);
            TransformationStepConfig sortProfile = sortProfile(customerSpace, PROFILE_TABLE_PREFIX);
            // -----------
            List<TransformationStepConfig> steps = Arrays.asList( //
                    aggregate, //
                    profile, //
                    bucket, //
                    calc, //
                    sortProfile //
            );
            // -----------
            request.setSteps(steps);
            return request;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private TransformationStepConfig aggregate(CustomerSpace customerSpace, String masterTablePrefix, 
                                               String transactionTableName, String accountTableName, Map<String, String> productMap) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_TRANSACTION_AGGREGATOR);

        String transactionSourceName = "Transaction";
        String accountSourceName = "Acount";
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

        String confStr = appendEngineConf(conf, heavyEngineConfig());
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

    private void upsertTables(String customerSpace, String masterTableName, String profileTableName) {
        Table masterTable = metadataProxy.getTable(customerSpace, masterTableName);
        if (masterTable == null) {
            throw new RuntimeException("Failed to find master table in customer " + customerSpace);
        } else {
            updateTable(customerSpace, masterTableName, masterTable);
        }
        Table profileTable = metadataProxy.getTable(customerSpace, profileTableName);
        if (profileTable == null) {
            throw new RuntimeException("Failed to find profile table in customer " + customerSpace);
        }
        DataCollection.Version inactiveVersion = dataCollectionProxy.getInactiveVersion(customerSpace);

        dataCollectionProxy.upsertTable(customerSpace, masterTableName, TableRoleInCollection.ConsolidatedPurchaseHistory, inactiveVersion);
        masterTable = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedPurchaseHistory, inactiveVersion);
        if (masterTable == null) {
            throw new IllegalStateException("Cannot find the upserted master table in data collection.");
        }

        dataCollectionProxy.upsertTable(customerSpace, profileTableName, TableRoleInCollection.PurchaseHistoryProfile, inactiveVersion);
        profileTable = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.PurchaseHistoryProfile, inactiveVersion);
        if (profileTable == null) {
            throw new IllegalStateException("Cannot find the upserted profile table in data collection.");
        }
    }


    private void updateTable(String customerSpace, String masterTableName, Table masterTable) {
        List<Attribute> attributes = masterTable.getAttributes();

        for (Attribute attribute : attributes) {
            String productId = TransactionMetrics.getProductIdFromAttr(attribute.getName());
            if (productId == null) {
                continue;
            }
            attribute.setCategory(Category.PRODUCT_SPEND.getName());
            attribute.setSubcategory(productMap.get(productId));
        }

        metadataProxy.updateTable(customerSpace, masterTableName, masterTable);
    }

    private Map<String, String> loadProduct(Table productTable) {
        Map<String, String> productMap = new HashMap<String, String>();
        for (Extract extract : productTable.getExtracts()) {
            List<GenericRecord> productList = AvroUtils.getDataFromGlob(yarnConfiguration, extract.getPath());
            for (GenericRecord product : productList) {
                productMap.put(product.get(InterfaceName.ProductId.name()).toString(), product.get(InterfaceName.ProductName.name()).toString());
            }
        }
        return productMap;
    }
}
