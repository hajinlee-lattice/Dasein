package com.latticeengines.cdl.workflow.steps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.SparkIOMetadataWrapper;
import com.latticeengines.domain.exposed.spark.cdl.TransformTxnStreamConfig;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.spark.exposed.job.cdl.BuildPeriodTransactionJob;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class BuildPeriodTransaction extends BaseProcessAnalyzeSparkStep<ProcessTransactionStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(BuildPeriodTransaction.class);

    private static final String AGG_PREFIX_FORMAT = "%s_" + TableRoleInCollection.AggregatedPeriodTransaction.name(); // tenantId
    private static final String CON_PREFIX_FORMAT = "Strategy%sConsolidatedPeriodTransaction"; // periodName
    private static final String PERIOD_STREAM_PREFIX = AggPeriodTransactionStep.PERIOD_TXN_PREFIX_FMT; // type, period

    private static final String accountId = InterfaceName.AccountId.name();
    private static final String productId = InterfaceName.ProductId.name();
    private static final String txnType = InterfaceName.TransactionType.name();
    private static final String rowCount = InterfaceName.__Row_Count__.name();
    private static final String amount = InterfaceName.Amount.name();
    private static final String quantity = InterfaceName.Quantity.name();
    private static final String cost = InterfaceName.Cost.name();

    private static final String productType = InterfaceName.ProductType.name();
    private static final String totalAmount = InterfaceName.TotalAmount.name();
    private static final String totalCost = InterfaceName.TotalCost.name();
    private static final String totalQuantity = InterfaceName.TotalQuantity.name();
    private static final String compositeKey = InterfaceName.__Composite_Key__.name();
    private static final String periodName = InterfaceName.PeriodName.name();
    private static final String periodId = InterfaceName.PeriodId.name();
    private static final String txnCount = InterfaceName.TransactionCount.name();

    private static final List<String> STANDARD_PERIOD_TXN_FIELDS = Arrays.asList(accountId, productId, productType,
            txnType, periodId, periodName, totalAmount, totalCost, totalQuantity, txnCount, compositeKey);

    private String rollingPeriod;

    private final String analytic = ProductType.Analytic.name();
    private final String spending = ProductType.Spending.name();

    @Inject
    private PeriodProxy periodProxy;

    @Override
    protected void bootstrap() {
        super.bootstrap();
        rollingPeriod = configuration.getApsRollingPeriod();
    }

    @Override
    public void execute() {
        bootstrap();
        log.info("Rolling period: {}", rollingPeriod);
        Map<String, Table> periodTransactionTables = getTablesFromMapCtxKey(customerSpaceStr, PERIOD_TXN_STREAMS);
        log.info("Retrieved period streams: {}",
                periodTransactionTables.entrySet().stream()
                        .map(entry -> Pair.of(entry.getKey(), entry.getValue().getName()))
                        .collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
        List<String> retainTypes = getListObjectFromContext(RETAIN_PRODUCT_TYPE, String.class);
        if (CollectionUtils.isEmpty(retainTypes)) {
            throw new IllegalStateException("No retain types found in context");
        }
        log.info("Found product type streams from earlier step: {}", retainTypes);

        cleanupInactiveVersion();
        buildConsolidatedPeriodTransaction(periodTransactionTables, retainTypes); // repartitioned by PeriodId
        buildAggregatedPeriodTransaction(periodTransactionTables, retainTypes); // repartitioned by PeriodId

        setTransactionRebuiltFlag();
    }

    private void cleanupInactiveVersion() {
        // cleanup tables cloned by updateTransaction
        log.info("cleanup tables cloned by updateTransaction");
        dataCollectionProxy.unlinkTables(customerSpaceStr, TableRoleInCollection.ConsolidatedPeriodTransaction, inactive);
        dataCollectionProxy.unlinkTables(customerSpaceStr, TableRoleInCollection.AggregatedPeriodTransaction, inactive);
    }

    private void buildConsolidatedPeriodTransaction(Map<String, Table> periodTransactionTables,
            List<String> retainTypes) {
        log.info("Building consolidated period transaction");
        Map<String, Table> batchStores = getTablesFromMapCtxKey(customerSpaceStr,
                PERIOD_TRXN_TABLE_NAMES_BY_PERIOD_NAME);
        if (tableExist(batchStores) && tableInHdfs(batchStores, false)) {
            log.info("Retrieved period transaction batch stores: {}. Going through shortcut mode.",
                    batchStores.entrySet().stream().map(entry -> {
                        String periodName = entry.getKey();
                        Table table = entry.getValue();
                        return Pair.of(periodName, table.getName());
                    }).collect(Collectors.toMap(Pair::getLeft, Pair::getRight)));
            dataCollectionProxy.upsertTables(customerSpaceStr,
                    batchStores.values().stream().map(Table::getName).collect(Collectors.toList()),
                    TableRoleInCollection.ConsolidatedPeriodTransaction, inactive);
            return;
        }

        // periodName -> tableName
        Map<String, String> signatureTableName = new HashMap<>();
        List<String> periodStrategies = periodProxy.getPeriodNames(customerSpaceStr);
        Map<String, Table> consolidatedPeriodTxnTables = periodStrategies.stream().map(periodName -> {
            SparkJobResult result = runSparkJob(BuildPeriodTransactionJob.class,
                    buildConsolidatedPeriodTxnConfig(periodName, retainTypes, periodTransactionTables));
            String prefix = String.format(CON_PREFIX_FORMAT, periodName);
            String tableName = NamingUtils.timestamp(prefix);
            Table table = toTable(tableName, compositeKey, result.getTargets().get(0));
            metadataProxy.createTable(customerSpaceStr, tableName, table);
            signatureTableName.put(periodName, table.getName());
            return Pair.of(periodName, table);
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        List<String> tableNames = new ArrayList<>(signatureTableName.values());
        log.info("Generated consolidated period stores: {}", tableNames);
        dataCollectionProxy.upsertTablesWithSignatures(customerSpaceStr, signatureTableName,
                TableRoleInCollection.ConsolidatedPeriodTransaction, inactive);
        exportToS3AndAddToContext(consolidatedPeriodTxnTables, PERIOD_TRXN_TABLE_NAMES_BY_PERIOD_NAME);
    }

    private void buildAggregatedPeriodTransaction(Map<String, Table> periodTransactionTables,
            List<String> retainTypes) {
        log.info("Building aggregated period transaction");
        Table servingStore = getTableSummaryFromKey(customerSpaceStr, AGG_PERIOD_TRXN_TABLE_NAME);
        if (tableExist(servingStore) && tableInHdfs(servingStore, false)) {
            String tableName = servingStore.getName();
            log.info("Retrieved period transaction serving store: {}. Going through shortcut mode.", tableName);
            dataCollectionProxy.upsertTable(customerSpaceStr, tableName,
                    TableRoleInCollection.AggregatedPeriodTransaction, inactive);
            exportTableRoleToRedshift(servingStore, TableRoleInCollection.AggregatedPeriodTransaction);
            return;
        }

        SparkJobResult result = runSparkJob(BuildPeriodTransactionJob.class,
                buildAggregatedPeriodTxnConfig(periodTransactionTables, retainTypes));
        String prefix = String.format(AGG_PREFIX_FORMAT, customerSpace.getTenantId());
        String tableName = NamingUtils.timestamp(prefix);
        Table table = toTable(tableName, compositeKey, result.getTargets().get(0));
        metadataProxy.createTable(customerSpaceStr, tableName, table);
        log.info("Generated aggregated period store: {}", table.getName());
        dataCollectionProxy.upsertTable(customerSpaceStr, tableName, TableRoleInCollection.AggregatedPeriodTransaction,
                inactive);
        exportToS3AndAddToContext(table, AGG_PERIOD_TRXN_TABLE_NAME);
        exportTableRoleToRedshift(table, TableRoleInCollection.AggregatedPeriodTransaction);
    }

    // build periodTxn serving store with all product type and all periods
    // TODO - remove this after PurchaseHistoryServiceImpl#getPeriodTransactionsByAccountId stable
//    private TransformTxnStreamConfig buildAggregatedPeriodTxnConfig(Map<String, Table> periodTransactionTables,
//                                                                    List<String> retainTypes) {
//        TransformTxnStreamConfig config = new TransformTxnStreamConfig();
//        config.compositeSrc = Arrays.asList(accountId, productId, productType, txnType, periodId, periodName);
//        config.renameMapping = constructPeriodRename();
//        config.targetColumns = STANDARD_PERIOD_TXN_FIELDS;
//        config.repartitionKey = periodId;
//        config.inputMetadataWrapper = new SparkIOMetadataWrapper();
//        List<DataUnit> inputs = new ArrayList<>();
//        periodProxy.getPeriodNames(customerSpaceStr).forEach(periodName -> {
//            config.inputMetadataWrapper.getMetadata().put(periodName, new SparkIOMetadataWrapper.Partition(inputs.size(), retainTypes));
//            retainTypes.forEach(type -> {
//                String periodStreamPrefix = String.format(PERIOD_STREAM_PREFIX, type, periodName);
//                Table table = periodTransactionTables.get(periodStreamPrefix);
//                inputs.add(table.partitionedToHdfsDataUnit(null, Collections.singletonList(periodId)));
//            });
//        });
//        config.setInput(inputs);
//        return config;
//    }

    private TransformTxnStreamConfig buildConsolidatedPeriodTxnConfig(String period, List<String> retainTypes,
            Map<String, Table> periodTransactionTables) {
        TransformTxnStreamConfig config = new TransformTxnStreamConfig();
        config.compositeSrc = Arrays.asList(accountId, productId, productType, txnType, periodId, periodName);
        config.renameMapping = constructPeriodRename();
        config.targetColumns = STANDARD_PERIOD_TXN_FIELDS;
        config.repartitionKey = periodId;
        config.inputMetadataWrapper = new SparkIOMetadataWrapper();
        config.inputMetadataWrapper.getMetadata().put(period, new SparkIOMetadataWrapper.Partition(0, null));
        List<DataUnit> inputs = new ArrayList<>();
        List<String> types = new ArrayList<>();
        retainTypes.forEach(type -> {
            String periodStreamPrefix = String.format(PERIOD_STREAM_PREFIX, type, period);
            if (periodTransactionTables.containsKey(periodStreamPrefix)) {
                Table table = periodTransactionTables.get(periodStreamPrefix);
                inputs.add(table.partitionedToHdfsDataUnit(null, Collections.singletonList(periodId)));
                types.add(type);
            }
        });
        config.inputMetadataWrapper.getMetadata().get(period).setLabels(types);
        config.setInput(inputs);
        return config;
    }

    // build periodTxn serving store with analytic rolling period + spending month
    private TransformTxnStreamConfig buildAggregatedPeriodTxnConfig(Map<String, Table> periodTransactionTables,
            List<String> retainTypes) {
        TransformTxnStreamConfig config = new TransformTxnStreamConfig();
        config.compositeSrc = Arrays.asList(accountId, productId, productType, txnType, periodId, periodName);
        config.renameMapping = constructPeriodRename();
        config.targetColumns = STANDARD_PERIOD_TXN_FIELDS;
        config.repartitionKey = periodId;
        config.inputMetadataWrapper = new SparkIOMetadataWrapper();
        List<DataUnit> inputs = new ArrayList<>();
        if (retainTypes.contains(analytic)) {
            config.inputMetadataWrapper.getMetadata().put(rollingPeriod, new SparkIOMetadataWrapper.Partition(0, null));
            List<String> rollingPeriodStreamTypes = new ArrayList<>();
            rollingPeriodStreamTypes.add(analytic);
            inputs.add(periodTransactionTables.get(String.format(PERIOD_STREAM_PREFIX, analytic, rollingPeriod))
                    .partitionedToHdfsDataUnit(null, Collections.singletonList(periodId)));
            config.inputMetadataWrapper.getMetadata().get(rollingPeriod).setLabels(rollingPeriodStreamTypes);
        }
        if (retainTypes.contains(spending)) {
            if (config.inputMetadataWrapper.getMetadata().containsKey(PeriodStrategy.Template.Month.name())) {
                // rollingPeriod == month, append to label list
                config.inputMetadataWrapper.getMetadata().get(PeriodStrategy.Template.Month.name()).getLabels()
                        .add(spending);
            } else {
                // add new partition for month period
                config.inputMetadataWrapper.getMetadata().put(PeriodStrategy.Template.Month.name(),
                        new SparkIOMetadataWrapper.Partition(inputs.size(), Collections.singletonList(spending)));
            }
            inputs.add(periodTransactionTables
                    .get(String.format(PERIOD_STREAM_PREFIX, spending, PeriodStrategy.Template.Month.name()))
                    .partitionedToHdfsDataUnit(null, Collections.singletonList(periodId)));
        }
        config.setInput(inputs);
        return config;
    }

    private Map<String, String> constructPeriodRename() {
        Map<String, String> map = new HashMap<>();
        map.put(amount, totalAmount);
        map.put(cost, totalCost);
        map.put(quantity, totalQuantity);
        map.put(rowCount, txnCount);
        return map;
    }

    private void setTransactionRebuiltFlag() {
        DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        if (BooleanUtils.isNotTrue(status.getTransactionRebuilt())) {
            log.info("Rebuild transaction finished, set TransactionRebuilt=true in data collection status");
            status.setTransactionRebuilt(true);
            putObjectInContext(CDL_COLLECTION_STATUS, status);
        } else {
            log.info("TransactionRebuilt flag already set to true");
        }
    }
}
