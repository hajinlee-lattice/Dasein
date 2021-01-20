package com.latticeengines.cdl.workflow.steps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.TransformTxnStreamConfig;
import com.latticeengines.spark.exposed.job.cdl.BuildDailyTransactionJob;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class BuildDailyTransaction extends BaseProcessAnalyzeSparkStep<ProcessTransactionStepConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(BuildDailyTransaction.class);

    private static final String SERVING_PREFIX = "%s_" + TableRoleInCollection.AggregatedTransaction.name(); // tenantId
    private static final String BATCH_PREFIX = TableRoleInCollection.ConsolidatedDailyTransaction.name();

    private static final String accountId = InterfaceName.AccountId.name();
    private static final String productId = InterfaceName.ProductId.name();
    private static final String txnType = InterfaceName.TransactionType.name();
    private static final String streamDate = InterfaceName.__StreamDate.name();
    private static final String streamDateId = InterfaceName.StreamDateId.name();
    private static final String rowCount = InterfaceName.__Row_Count__.name();
    private static final String amount = InterfaceName.Amount.name();
    private static final String quantity = InterfaceName.Quantity.name();
    private static final String cost = InterfaceName.Cost.name();

    private static final String productType = InterfaceName.ProductType.name();
    private static final String txnDate = InterfaceName.TransactionDate.name();
    private static final String txnDayPeriod = InterfaceName.TransactionDayPeriod.name();
    private static final String totalAmount = InterfaceName.TotalAmount.name();
    private static final String totalCost = InterfaceName.TotalCost.name();
    private static final String totalQuantity = InterfaceName.TotalQuantity.name();
    private static final String compositeKey = InterfaceName.__Composite_Key__.name();
    private static final String periodName = InterfaceName.PeriodName.name();
    private static final String txnCount = InterfaceName.TransactionCount.name();

    private static final List<String> STANDARD_FIELDS = Arrays.asList(accountId, productId, productType, txnType,
            txnDate, txnDayPeriod, totalAmount, totalCost, totalQuantity, txnCount, periodName, compositeKey);

    @Override
    public void execute() {
        bootstrap();
        Map<String, Table> dailyTxnStream = getTablesFromMapCtxKey(customerSpaceStr, DAILY_TXN_STREAMS);
        log.info("Retrieved daily streams: {}",
                dailyTxnStream.entrySet().stream().map(entry -> Pair.of(entry.getKey(), entry.getValue().getName()))
                        .collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
        List<String> retainTypes = getListObjectFromContext(RETAIN_PRODUCT_TYPE, String.class);
        if (CollectionUtils.isEmpty(retainTypes)) {
            throw new IllegalStateException("No retain types found in context");
        }
        log.info("Retaining transactions of product type: {}", retainTypes);
        cleanupInactiveVersion();
        buildTransactionBatchStore(dailyTxnStream, retainTypes); // ConsolidatedDaily, partitioned and repartitioned by txnDayPeriod
        buildTransactionServingStore(dailyTxnStream, retainTypes); // Aggregated, no partition, repartitioned by txnDayPeriod
    }

    private void cleanupInactiveVersion() {
        log.info("cleanup tables cloned by updateTransaction");
        dataCollectionProxy.unlinkTables(customerSpaceStr, TableRoleInCollection.ConsolidatedDailyTransaction, inactive);
        dataCollectionProxy.unlinkTables(customerSpaceStr, TableRoleInCollection.AggregatedTransaction, inactive);
    }

    private void buildTransactionBatchStore(Map<String, Table> dailyTxnStream, List<String> retainTypes) {
        Table dailyTxnBatchStoreTable = getTableSummaryFromKey(customerSpaceStr, DAILY_TRXN_TABLE_NAME);
        if (tableExist(dailyTxnBatchStoreTable) && tableInHdfs(dailyTxnBatchStoreTable, true)) {
            String tableName = dailyTxnBatchStoreTable.getName();
            log.info("Retrieved daily transaction batch store from context: {}. Going through shortcut mode.",
                    tableName);
            dataCollectionProxy.upsertTable(customerSpaceStr, tableName,
                    TableRoleInCollection.ConsolidatedDailyTransaction, inactive);
        } else {
            SparkJobResult result = runSparkJob(BuildDailyTransactionJob.class,
                    getSparkConfig(txnDayPeriod, txnDayPeriod, dailyTxnStream, retainTypes));
            saveBatchStore(result.getTargets().get(0));
        }
    }

    private void saveBatchStore(HdfsDataUnit consolidatedTxnDU) {
        String tableName = NamingUtils.timestamp(BATCH_PREFIX);
        Table consolidatedTxnTable = dirToTable(tableName, compositeKey, consolidatedTxnDU);
        metadataProxy.createTable(customerSpaceStr, tableName, consolidatedTxnTable);
        dataCollectionProxy.upsertTable(customerSpaceStr, tableName, TableRoleInCollection.ConsolidatedDailyTransaction,
                inactive);
        exportToS3AndAddToContext(consolidatedTxnTable, DAILY_TRXN_TABLE_NAME);
    }

    private void buildTransactionServingStore(Map<String, Table> dailyTxnStream, List<String> retainTypes) {
        Table dailyTxnServingStoreTable = getTableSummaryFromKey(customerSpaceStr, AGG_DAILY_TRXN_TABLE_NAME);
        if (tableExist(dailyTxnServingStoreTable) && tableInHdfs(dailyTxnServingStoreTable, false)) {
            String tableName = dailyTxnServingStoreTable.getName();
            log.info("Retrieved daily transaction serving store from context: {}. Going through shortcut mode.",
                    tableName);
            dataCollectionProxy.upsertTable(customerSpaceStr, tableName, TableRoleInCollection.AggregatedTransaction,
                    inactive);
            exportTableRoleToRedshift(dailyTxnServingStoreTable, TableRoleInCollection.AggregatedTransaction);
        } else {
            SparkJobResult result = runSparkJob(BuildDailyTransactionJob.class,
                    getSparkConfig(null, txnDayPeriod, dailyTxnStream, retainTypes));
            saveServingStore(result.getTargets().get(0));
        }
    }

    private void saveServingStore(HdfsDataUnit aggregatedTxnDU) {
        String prefix = String.format(SERVING_PREFIX, customerSpace.getTenantId());
        String tableName = NamingUtils.timestamp(prefix);
        Table aggregatedTxnTable = toTable(tableName, compositeKey, aggregatedTxnDU);
        metadataProxy.createTable(customerSpaceStr, tableName, aggregatedTxnTable);
        dataCollectionProxy.upsertTable(customerSpaceStr, tableName, TableRoleInCollection.AggregatedTransaction,
                inactive);
        exportToS3AndAddToContext(aggregatedTxnTable, AGG_DAILY_TRXN_TABLE_NAME);
        exportTableRoleToRedshift(aggregatedTxnTable, TableRoleInCollection.AggregatedTransaction);
    }

    private TransformTxnStreamConfig getSparkConfig(String partitionKey, String repartitionKey,
            Map<String, Table> dailyTxnStream, List<String> retainTypes) {
        TransformTxnStreamConfig config = new TransformTxnStreamConfig();
        config.compositeSrc = Arrays.asList(accountId, productId, productType, txnType, txnDate, txnDayPeriod);
        config.renameMapping = constructDailyRename();
        config.targetColumns = STANDARD_FIELDS;
        config.partitionKey = partitionKey;
        config.repartitionKey = repartitionKey;
        config.retainTypes = retainTypes;

        List<DataUnit> inputs = new ArrayList<>();
        retainTypes.forEach(type -> {
            Table table = dailyTxnStream.get(type);
            inputs.add(table.partitionedToHdfsDataUnit(null,
                    Collections.singletonList(InterfaceName.StreamDateId.name())));
        });
        config.setInput(inputs);
        return config;
    }

    private Map<String, String> constructDailyRename() {
        Map<String, String> map = new HashMap<>();
        map.put(streamDate, txnDate);
        map.put(streamDateId, txnDayPeriod);
        map.put(amount, totalAmount);
        map.put(cost, totalCost);
        map.put(quantity, totalQuantity);
        map.put(rowCount, txnCount);
        return map;
    }
}
