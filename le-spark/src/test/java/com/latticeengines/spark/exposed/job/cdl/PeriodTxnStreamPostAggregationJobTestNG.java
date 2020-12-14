package com.latticeengines.spark.exposed.job.cdl;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.PeriodTxnStreamPostAggregationConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;
import com.latticeengines.spark.util.DeriveAttrsUtils;

public class PeriodTxnStreamPostAggregationJobTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PeriodTxnStreamPostAggregationJobTestNG.class);

    private static final String accountId = InterfaceName.AccountId.name();
    private static final String productId = InterfaceName.ProductId.name();
    private static final String txnType = InterfaceName.TransactionType.name();
    private static final String rowCount = InterfaceName.__Row_Count__.name();
    private static final String lastActDate = InterfaceName.LastActivityDate.name();
    private static final String amount = InterfaceName.Amount.name();
    private static final String quantity = InterfaceName.Quantity.name();
    private static final String cost = InterfaceName.Cost.name();
    private static final String version = DeriveAttrsUtils.VERSION_COL();

    private static final String productType = InterfaceName.ProductType.name();
    private static final String periodId = InterfaceName.PeriodId.name();

    private static final String analytic = ProductType.Analytic.name();

    private static final int minPeriod = 10;
    private static final int maxPeriod = 20;

    @Test(groups = "functional")
    private void test() {
        SparkJobResult result = runSparkJob(PeriodTxnStreamPostAggregationJob.class,
                new PeriodTxnStreamPostAggregationConfig(), Collections.singletonList(setupAnalyticMonthPeriod()),
                getWorkspace());
        verify(result, Collections.singletonList(this::verifyRecordCount));
    }

    @Test(groups = "functional")
    private void testWrongColumnType() {
        SparkJobResult result = runSparkJob(PeriodTxnStreamPostAggregationJob.class,
                new PeriodTxnStreamPostAggregationConfig(), Collections.singletonList(setupAnalyticMonthPeriodWithWrongColumn()),
                getWorkspace());
    }

    private String setupAnalyticMonthPeriod() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(accountId, String.class), //
                Pair.of(productId, String.class), //
                Pair.of(txnType, String.class), //
                Pair.of(productType, String.class), //
                Pair.of(periodId, Integer.class), //
                Pair.of(rowCount, Integer.class), //
                Pair.of(amount, Double.class), //
                Pair.of(quantity, Integer.class), //
                Pair.of(cost, Double.class), //
                Pair.of(lastActDate, Long.class), // not useful for txn
                Pair.of(version, Long.class) // not useful for txn
        );
        Object[][] data = new Object[][] { // should fill in [11, 19]
                { "a1", "p1", "Purchase", analytic, minPeriod, 15, 16.0, 4, 4.0, null, 0L }, //
                { "a1", "p1", "Purchase", analytic, maxPeriod, 15, 16.0, 4, 4.0, null, 0L } //
        };
        return uploadHdfsDataUnit(data, fields);
    }

    private String setupAnalyticMonthPeriodWithWrongColumn() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(accountId, String.class), //
                Pair.of(productId, String.class), //
                Pair.of(txnType, String.class), //
                Pair.of(productType, String.class), //
                Pair.of(periodId, Integer.class), //
                Pair.of(rowCount, Integer.class), //
                Pair.of(amount, Double.class), //
                Pair.of(quantity, Integer.class), //
                Pair.of(cost, Long.class), // wrong type since it is null in upstream
                Pair.of(lastActDate, Long.class), // not useful for txn
                Pair.of(version, Long.class) // not useful for txn
        );
        Object[][] data = new Object[][] { // should fill in [11, 19]
                { "a1", "p1", "Purchase", analytic, minPeriod, 15, 16.0, 4, null, null, 0L }, //
                { "a1", "p1", "Purchase", analytic, maxPeriod, 15, 16.0, 4, null, null, 0L } //
        };
        return uploadHdfsDataUnit(data, fields);
    }

    private Boolean verifyRecordCount(HdfsDataUnit output) {
        Assert.assertEquals(output.getCount(), Long.valueOf(maxPeriod - minPeriod + 1));
        Iterator<GenericRecord> iterator = verifyAndReadTarget(output);
        return true;
    }
}
