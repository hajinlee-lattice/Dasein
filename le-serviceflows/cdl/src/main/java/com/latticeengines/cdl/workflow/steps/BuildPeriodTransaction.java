package com.latticeengines.cdl.workflow.steps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Pair;
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
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.TransformTxnStreamConfig;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.spark.exposed.job.cdl.TransformTxnStreamJob;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class BuildPeriodTransaction extends BaseProcessAnalyzeSparkStep<ProcessTransactionStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(BuildPeriodTransaction.class);

    private static final String AGG_PREFIX_FORMAT = "%s_" + TableRoleInCollection.AggregatedPeriodTransaction.name(); // tenantId
    private static final String CON_PREFIX_FORMAT = "Strategy%sConsolidatedPeriodTransaction"; // periodName
    private static final String PERIOD_STREAM_PREFIX = "PERIOD_TXN_%s_%s"; // type, period

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

    // FIXME - in db there are cols (e.g. txnDate) not in avro. not having
    // those here breaks anything?
    private static final List<String> STANDARD_PERIOD_TXN_FIELDS = Arrays.asList(accountId, productId, productType,
            txnType, periodId, periodName, totalAmount, totalCost, totalQuantity, txnCount, compositeKey);

    @Inject
    private PeriodProxy periodProxy;

    private List<String> periodStrategies;

    @Override
    protected void bootstrap() {
        super.bootstrap();
        periodStrategies = periodProxy.getPeriodNames(customerSpaceStr);
    }

    @Override
    public void execute() {
        bootstrap();
        // TODO - retry
        Map<String, Table> periodTransactionTables = getTablesFromMapCtxKey(customerSpaceStr, PERIOD_TXN_STREAMS);
        buildConsolidatedPeriodTransaction(periodTransactionTables);
        buildAggregatedPeriodTransaction(periodTransactionTables);
    }

    private void buildConsolidatedPeriodTransaction(Map<String, Table> periodTransactionTables) {
        log.info("Building consolidated period transaction");
        // TODO - maybe share a livy session?
        Map<String, Table> consolidatedPeriodTxnTables = periodStrategies.stream().map(periodName -> {
            SparkJobResult result = runSparkJob(TransformTxnStreamJob.class,
                    buildConsolidatedPeriodTxnConfig(periodName, periodTransactionTables));
            String prefix = String.format(CON_PREFIX_FORMAT, periodName);
            String tableName = TableUtils.getFullTableName(prefix,
                    HashUtils.getCleanedString(UuidUtils.shortenUuid(UUID.randomUUID())));
            Table table = toTable(tableName, compositeKey, result.getTargets().get(0));
            metadataProxy.createTable(customerSpaceStr, tableName, table);
            return Pair.of(periodName, table);
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        List<String> tableNames = consolidatedPeriodTxnTables.values().stream().map(Table::getName)
                .collect(Collectors.toList());
        log.info("Generated consolidated period stores: {}", tableNames);
        dataCollectionProxy.upsertTables(customerSpaceStr, tableNames,
                TableRoleInCollection.ConsolidatedPeriodTransaction, inactive);
        exportToS3AndAddToContext(consolidatedPeriodTxnTables, PERIOD_TRXN_TABLE_NAME);
    }

    private void buildAggregatedPeriodTransaction(Map<String, Table> periodTransactionTables) {
        log.info("Building aggregated period transaction");
        SparkJobResult result = runSparkJob(TransformTxnStreamJob.class,
                buildAggregatedPeriodTxnConfig(periodTransactionTables));
        String prefix = String.format(AGG_PREFIX_FORMAT, customerSpace.getTenantId());
        String tableName = TableUtils.getFullTableName(prefix,
                HashUtils.getCleanedString(UuidUtils.shortenUuid(UUID.randomUUID())));
        Table table = toTable(tableName, compositeKey, result.getTargets().get(0));
        metadataProxy.createTable(customerSpaceStr, tableName, table);
        log.info("Generated aggregated period store: {}", table.getName());
        dataCollectionProxy.upsertTable(customerSpaceStr, tableName, TableRoleInCollection.AggregatedPeriodTransaction,
                inactive);
        exportToS3AndAddToContext(table, AGG_PERIOD_TRXN_TABLE_NAME);
        exportTableRoleToRedshift(table, TableRoleInCollection.AggregatedPeriodTransaction);
    }

    private TransformTxnStreamConfig buildConsolidatedPeriodTxnConfig(String period,
            Map<String, Table> periodTransactionTables) {
        TransformTxnStreamConfig config = new TransformTxnStreamConfig();
        config.compositeSrc = Arrays.asList(accountId, productId, productType, txnType, periodId, periodName);
        config.renameMapping = constructPeriodRename();
        config.inputPeriods = Collections.singletonList(period);
        config.targetColumns = STANDARD_PERIOD_TXN_FIELDS;

        String analyticPrefix = String.format(PERIOD_STREAM_PREFIX, ProductType.Analytic.name(), period);
        String spendingPrefix = String.format(PERIOD_STREAM_PREFIX, ProductType.Spending.name(), period);
        Table analyticStream = periodTransactionTables.get(analyticPrefix);
        Table spendingStream = periodTransactionTables.get(spendingPrefix);
        log.info("Retrieved period streams analytic={}, spending={}", analyticStream.getName(),
                spendingStream.getName());
        config.setInput(Arrays.asList( //
                analyticStream.partitionedToHdfsDataUnit(null, Collections.singletonList(periodId)),
                spendingStream.partitionedToHdfsDataUnit(null, Collections.singletonList(periodId))));
        return config;
    }

    private TransformTxnStreamConfig buildAggregatedPeriodTxnConfig(Map<String, Table> periodTransactionTables) {
        TransformTxnStreamConfig config = new TransformTxnStreamConfig();
        config.compositeSrc = Arrays.asList(accountId, productId, productType, txnType, periodId, periodName);
        config.renameMapping = constructPeriodRename();
        config.inputPeriods = periodStrategies;
        config.targetColumns = STANDARD_PERIOD_TXN_FIELDS;

        List<DataUnit> inputs = new ArrayList<>();
        periodStrategies.forEach(periodName -> {
            String analyticPrefix = String.format(PERIOD_STREAM_PREFIX, ProductType.Analytic.name(), periodName);
            String spendingPrefix = String.format(PERIOD_STREAM_PREFIX, ProductType.Spending.name(), periodName);
            Table analyticStream = periodTransactionTables.get(analyticPrefix);
            Table spendingStream = periodTransactionTables.get(spendingPrefix);
            log.info("Retrieved {} streams analytic={}, spending={}", periodName, analyticStream.getName(),
                    spendingStream.getName());
            inputs.addAll(Arrays.asList( //
                    analyticStream.partitionedToHdfsDataUnit(null, Collections.singletonList(periodId)),
                    spendingStream.partitionedToHdfsDataUnit(null, Collections.singletonList(periodId))));
        });
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
}
