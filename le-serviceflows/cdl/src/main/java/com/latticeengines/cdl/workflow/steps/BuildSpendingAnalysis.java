package com.latticeengines.cdl.workflow.steps;

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
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.SparkIOMetadataWrapper;
import com.latticeengines.domain.exposed.spark.cdl.TransformTxnStreamConfig;
import com.latticeengines.spark.exposed.job.cdl.BuildPeriodTransactionJob;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class BuildSpendingAnalysis extends BaseProcessAnalyzeSparkStep<ProcessTransactionStepConfiguration> {

    // TODO - tables built in this step already been built by previous steps but in
    // avro format. Can change those to parquet and remove this step after avro no
    // longer needed

    private static final Logger log = LoggerFactory.getLogger(BuildSpendingAnalysis.class);

    private static final String PERIOD_STREAM_PREFIX = AggPeriodTransactionStep.PERIOD_TXN_PREFIX_FMT; // type, period
    private static final String SPENDING_ANALYSIS_PREFIX = "%s_MonthSpendingAnalysis"; // tenantId

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

    @Override
    public void execute() {
        bootstrap();
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
        if (!retainTypes.contains(ProductType.Spending.name())) {
            log.info("No spending product stream found. Skip building spending analysis");
            return;
        }
        String inputStreamKey = String.format(PERIOD_STREAM_PREFIX, ProductType.Spending.name(),
                PeriodStrategy.Template.Month.name());
        log.info("Input stream key: {}", inputStreamKey);
        if (!periodTransactionTables.containsKey(inputStreamKey)) {
            log.info("No spending month stream found. Skip building spending analysis");
            return;
        }
        // TODO - skip if flag SpendingAnalysis is off
        buildSpendingAnalysisPeriodStore(periodTransactionTables.get(inputStreamKey)); // repartitioned by periodId
        markNewPublication();
    }

    private void markNewPublication() {
        DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        status.setSpendingAnalysisPublished(true);
        putObjectInContext(CDL_COLLECTION_STATUS, status);
    }

    private void buildSpendingAnalysisPeriodStore(Table stream) {
        log.info("Building spending analysis period table");
        Table servingTable = getTableSummaryFromKey(customerSpaceStr, SPENDING_ANALYSIS_PERIOD_TABLE_NAME);
        if (tableExist(servingTable) && tableInHdfs(servingTable, false)) {
            log.info("Retrieved table: {}. Going through shortcut mode.", servingTable.getName());
            saveSpendingAnalysisTable(servingTable);
            return;
        }
        SparkJobResult result = runSparkJob(BuildPeriodTransactionJob.class, buildSparkConfig(stream));
        String prefix = String.format(SPENDING_ANALYSIS_PREFIX, customerSpace.getTenantId());
        String tableName = NamingUtils.timestamp(prefix);
        Table table = toTable(tableName, compositeKey, result.getTargets().get(0));
        metadataProxy.createTable(customerSpaceStr, tableName, table);
        saveSpendingAnalysisTable(table);
    }

    private TransformTxnStreamConfig buildSparkConfig(Table inputStream) {
        TransformTxnStreamConfig config = new TransformTxnStreamConfig();
        config.compositeSrc = Arrays.asList(accountId, productId, productType, txnType, periodId, periodName);
        config.renameMapping = constructPeriodRename();
        config.targetColumns = STANDARD_PERIOD_TXN_FIELDS;
        config.repartitionKey = periodId;
        config.inputMetadataWrapper = new SparkIOMetadataWrapper();
        config.inputMetadataWrapper.getMetadata().put(PeriodStrategy.Template.Month.name(),
                new SparkIOMetadataWrapper.Partition(0, Collections.singletonList(ProductType.Spending.name())));
        config.setInput(Collections
                .singletonList(inputStream.partitionedToHdfsDataUnit(null, Collections.singletonList(periodId))));
        config.outputParquet = true;
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

    private void saveSpendingAnalysisTable(Table table) {
        String tableName = table.getName();
        log.info("Saving spending analysis table {}", tableName);
        dataCollectionProxy.upsertTable(customerSpaceStr, tableName, TableRoleInCollection.SpendingAnalysisPeriod,
                inactive);
        exportToS3AndAddToContext(table, SPENDING_ANALYSIS_PERIOD_TABLE_NAME);
    }

}
