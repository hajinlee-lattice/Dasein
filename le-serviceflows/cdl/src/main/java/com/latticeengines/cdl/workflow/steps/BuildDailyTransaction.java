package com.latticeengines.cdl.workflow.steps;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HashUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.TransformTxnStreamConfig;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.spark.exposed.job.cdl.TransformTxnStreamJob;

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
        Table analyticDailyStream = dailyTxnStream.get(ProductType.Analytic.name());
        Table spendingDailyStream = dailyTxnStream.get(ProductType.Spending.name());
        log.info("Retrieved analytic stream {} and spending stream{}", analyticDailyStream.getName(),
                spendingDailyStream.getName());

        buildTransactionBatchStore(dailyTxnStream); // ConsolidatedDaily, partitioned by txnDayPeriod
        buildTransactionServingStore(dailyTxnStream); // Aggregated, no partition
    }

    private void buildTransactionBatchStore(Map<String, Table> dailyTxnStream) {
        SparkJobResult result = runSparkJob(TransformTxnStreamJob.class, getSparkConfig(txnDayPeriod, dailyTxnStream));
        saveBatchStore(result.getTargets().get(0));
    }

    private void saveBatchStore(HdfsDataUnit consolidatedTxnDU) {
        String tableName = TableUtils.getFullTableName(BATCH_PREFIX,
                HashUtils.getCleanedString(UuidUtils.shortenUuid(UUID.randomUUID())));
        Table consolidatedTxnTable = dirToTable(tableName, compositeKey, consolidatedTxnDU);
        metadataProxy.createTable(customerSpaceStr, tableName, consolidatedTxnTable);
        dataCollectionProxy.upsertTable(customerSpaceStr, tableName, TableRoleInCollection.ConsolidatedDailyTransaction,
                inactive);
        exportToS3AndAddToContext(consolidatedTxnTable, DAILY_TRXN_TABLE_NAME);
    }

    private void buildTransactionServingStore(Map<String, Table> dailyTxnStream) {
        SparkJobResult result = runSparkJob(TransformTxnStreamJob.class, getSparkConfig(null, dailyTxnStream));
        saveServingStore(result.getTargets().get(0));
    }

    private void saveServingStore(HdfsDataUnit aggregatedTxnDU) {
        String prefix = String.format(SERVING_PREFIX, customerSpace.getTenantId());
        String tableName = TableUtils.getFullTableName(prefix,
                HashUtils.getCleanedString(UuidUtils.shortenUuid(UUID.randomUUID())));
        Table aggregatedTxnTable = toTable(tableName, compositeKey, aggregatedTxnDU);
        metadataProxy.createTable(customerSpaceStr, tableName, aggregatedTxnTable);
        dataCollectionProxy.upsertTable(customerSpaceStr, tableName, TableRoleInCollection.AggregatedTransaction,
                inactive);
        exportToS3AndAddToContext(aggregatedTxnTable, AGG_DAILY_TRXN_TABLE_NAME);
        exportTableRoleToRedshift(aggregatedTxnTable, TableRoleInCollection.AggregatedTransaction);
    }

    private TransformTxnStreamConfig getSparkConfig(String partitionKey, Map<String, Table> dailyTxnStream) {
        TransformTxnStreamConfig config = new TransformTxnStreamConfig();
        config.compositeSrc = Arrays.asList(accountId, productId, productType, txnType, txnDate, txnDayPeriod);
        config.renameMapping = constructDailyRename();
        config.targetColumns = STANDARD_FIELDS;
        config.partitionKey = partitionKey;

        Table analyticDailyStream = dailyTxnStream.get(ProductType.Analytic.name());
        Table spendingDailyStream = dailyTxnStream.get(ProductType.Spending.name());
        config.setInput(Arrays.asList(
                analyticDailyStream.partitionedToHdfsDataUnit(null,
                        Collections.singletonList(InterfaceName.StreamDateId.name())),
                spendingDailyStream.partitionedToHdfsDataUnit(null,
                        Collections.singletonList(InterfaceName.StreamDateId.name()))));
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
