package com.latticeengines.spark.exposed.job.cdl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.TransformTxnStreamConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;
import com.latticeengines.spark.util.DeriveAttrsUtils;

public class BuildDailyTransactionJobTestNG extends SparkJobFunctionalTestNGBase {
    private static final String accountId = InterfaceName.AccountId.name();
    private static final String productId = InterfaceName.ProductId.name();
    private static final String txnType = InterfaceName.TransactionType.name();
    private static final String streamDate = InterfaceName.__StreamDate.name();
    private static final String streamDateId = InterfaceName.StreamDateId.name();
    private static final String rowCount = InterfaceName.__Row_Count__.name();
    private static final String lastActDate = InterfaceName.LastActivityDate.name();
    private static final String amount = InterfaceName.Amount.name();
    private static final String quantity = InterfaceName.Quantity.name();
    private static final String cost = InterfaceName.Cost.name();
    private static final String version = DeriveAttrsUtils.VERSION_COL();

    private static final String productType = InterfaceName.ProductType.name();
    private static final String txnDate = InterfaceName.TransactionDate.name();
    private static final String txnDayPeriod = InterfaceName.TransactionDayPeriod.name();
    private static final String totalAmount = InterfaceName.TotalAmount.name();
    private static final String totalCost = InterfaceName.TotalCost.name();
    private static final String totalQuantity = InterfaceName.TotalQuantity.name();
    private static final String compositeKey = InterfaceName.__Composite_Key__.name();
    private static final String periodName = InterfaceName.PeriodName.name();
    private static final String txnCount = InterfaceName.TransactionCount.name();

    private static final String analytic = ProductType.Analytic.name();
    private static final String spending = ProductType.Spending.name();

    private static final String DAY_1 = "2019-07-01";
    private static final String DAY_2 = "2019-07-02";
    private static final int DAY_1_PERIOD = 49729;
    private static final int DAY_2_PERIOD = 49730;

    private static final List<String> DAILY_TXN_FIELDS = Arrays.asList(accountId, productId, productType, txnType,
            txnDate, txnDayPeriod, totalAmount, totalCost, totalQuantity, txnCount, periodName, compositeKey);

    @Test(groups = "functional")
    private void testAggregatedTxnBothTypes() {
        List<String> inputs = Arrays.asList(setupSpendingDaily(), setupAnalyticDaily());
        TransformTxnStreamConfig config = new TransformTxnStreamConfig();
        config.compositeSrc = Arrays.asList(accountId, productId, productType, txnType, txnDate, txnDayPeriod);
        config.renameMapping = constructDailyRename();
        config.targetColumns = DAILY_TXN_FIELDS;
        config.retainTypes = Arrays.asList(analytic, spending);
        SparkJobResult result = runSparkJob(BuildDailyTransactionJob.class, config, inputs, getWorkspace());
        verify(result, Collections.singletonList(this::verifyOutputDailyFields));
    }

    @Test(groups = "functional")
    private void testAggregatedTxnAnalytic() {
        List<String> inputs = Collections.singletonList(setupAnalyticDaily());
        TransformTxnStreamConfig config = new TransformTxnStreamConfig();
        config.compositeSrc = Arrays.asList(accountId, productId, productType, txnType, txnDate, txnDayPeriod);
        config.renameMapping = constructDailyRename();
        config.targetColumns = DAILY_TXN_FIELDS;
        config.retainTypes = Collections.singletonList(analytic);
        SparkJobResult result = runSparkJob(BuildDailyTransactionJob.class, config, inputs, getWorkspace());
        verify(result, Collections.singletonList(this::verifyOutputDailyFields));
    }

    private String setupSpendingDaily() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(accountId, String.class), //
                Pair.of(productId, String.class), //
                Pair.of(txnType, String.class), //
                Pair.of(productType, String.class), //
                Pair.of(streamDate, String.class), //
                Pair.of(streamDateId, Integer.class), //
                Pair.of(rowCount, Integer.class), //
                Pair.of(amount, Double.class), //
                Pair.of(quantity, Integer.class), //
                Pair.of(cost, Double.class), //
                Pair.of(lastActDate, Long.class), // not useful for txn
                Pair.of(version, Long.class) // not useful for txn
        );
        Object[][] data = new Object[][] { //
                { "a1", "p1", "Purchase", spending, DAY_1, DAY_1_PERIOD, 2, 10.0, 1, 0.1, 0L, 0L }, //
                { "a1", "p1", "Purchase", spending, DAY_2, DAY_2_PERIOD, 1, 70.0, 7, 0.7, 0L, 0L }, //
                { "a1", "p2", "Purchase", spending, DAY_1, DAY_1_PERIOD, 1, 60.0, 6, 0.6, 0L, 0L } //
        };
        return uploadHdfsDataUnit(data, fields);
    }

    private String setupAnalyticDaily() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(accountId, String.class), //
                Pair.of(productId, String.class), //
                Pair.of(txnType, String.class), //
                Pair.of(productType, String.class), //
                Pair.of(streamDate, String.class), //
                Pair.of(streamDateId, Integer.class), //
                Pair.of(rowCount, Integer.class), //
                Pair.of(amount, Double.class), //
                Pair.of(quantity, Integer.class), //
                Pair.of(cost, Double.class), //
                Pair.of(lastActDate, Long.class), // not useful for txn
                Pair.of(version, Long.class) // not useful for txn
        );
        Object[][] data = new Object[][] { //
                { "a1", "p1", "Purchase", analytic, DAY_1, DAY_1_PERIOD, 2, 10.0, 1, 0.1, 0L, 0L }, //
                { "a1", "p1", "Purchase", analytic, DAY_2, DAY_2_PERIOD, 1, 70.0, 7, 0.7, 0L, 0L }, //
                { "a1", "p2", "Purchase", analytic, DAY_1, DAY_1_PERIOD, 1, 60.0, 6, 0.6, 0L, 0L } //
        };
        return uploadHdfsDataUnit(data, fields);
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

    private Boolean verifyOutputDailyFields(HdfsDataUnit output) {
        Iterator<GenericRecord> iterator = verifyAndReadTarget(output);
        Set<String> outputFields = new HashSet<>(DAILY_TXN_FIELDS);
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iterator) {
            Assert.assertEquals(
                    record.getSchema().getFields().stream().map(Schema.Field::name).collect(Collectors.toSet()),
                    outputFields);
        }
        return true;
    }
}
