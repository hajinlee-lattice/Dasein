package com.latticeengines.spark.exposed.job.cdl;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.DailyTxnStreamPostAggregationConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;
import com.latticeengines.spark.util.DeriveAttrsUtils;

public class DailyTxnStreamPostAggregationJobTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DailyTxnStreamPostAggregationJobTestNG.class);

    private static final String accountId = InterfaceName.AccountId.name();
    private static final String productId = InterfaceName.ProductId.name();
    private static final String productBundleId = InterfaceName.ProductBundleId.name();
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
    private static final String periodId = InterfaceName.PeriodId.name();
    private static final String txnCount = InterfaceName.TransactionCount.name();

    private static final String DAY_1 = "2019-07-01";
    private static final String DAY_2 = "2019-07-10";
    private static final int DAY_1_PERIOD = 49729;
    private static final int DAY_2_PERIOD = 49730;
    private static final String spending = ProductType.Spending.name();
    private static final String analytic = ProductType.Analytic.name();
    private static final String week = PeriodStrategy.Template.Week.name();
    private static final String month = PeriodStrategy.Template.Month.name();

    @Test(groups = "functional")
    private void test() {
        DailyTxnStreamPostAggregationConfig config = new DailyTxnStreamPostAggregationConfig();
        List<String> inputs = Arrays.asList(setupDailyStream(), setupProduct());
        SparkJobResult result = runSparkJob(DailyTxnStreamPostAggregationJob.class, config, inputs, getWorkspace());
    }

    private String setupDailyStream() {
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
                { "a1", "p1", "Purchase", "Analytic", DAY_1, DAY_1_PERIOD, 2, 10.0, 1, 0.1, 0L, 0L }, //
                { "a1", "p1", "Purchase", "Analytic", DAY_2, DAY_2_PERIOD, 1, 70.0, 7, 0.7, 0L, 0L } //
        };
        return uploadHdfsDataUnit(data, fields);
    }

    private String setupProduct() {
        List<Pair<String, Class<?>>> fields = Arrays.asList(
                Pair.of(productId, String.class), //
                Pair.of(productBundleId, String.class)
        );
        Object[][] data = new Object[][] { //
                {"p1", "pb1"}, //
                {"p2", "pb1"}, //
                {"p2", "pb2"} // fill missing pb2 to stream
        };
        return uploadHdfsDataUnit(data, fields);
    }
}
