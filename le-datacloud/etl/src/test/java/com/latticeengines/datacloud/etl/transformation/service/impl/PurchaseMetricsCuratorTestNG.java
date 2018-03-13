package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.domain.exposed.cdl.PeriodBuilderFactory;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ActivityMetricsCuratorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.TimeFilter.Period;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

public class PurchaseMetricsCuratorTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(PurchaseMetricsCuratorTestNG.class);

    private GeneralSource account = new GeneralSource("Account");
    private GeneralSource product = new GeneralSource("Product");
    private GeneralSource weekTable = new GeneralSource("WeekTable");
    private GeneralSource monthTable = new GeneralSource("MonthTable");
    private GeneralSource quarterTable = new GeneralSource("QuarterTable");
    private GeneralSource yearTable = new GeneralSource("YearTable");
    private GeneralSource cleanedWeekTable = new GeneralSource("CleanedWeekTable");
    private GeneralSource cleanedMonthTable = new GeneralSource("CleanedMonthTable");
    private GeneralSource cleanedQuarterTable = new GeneralSource("CleanedQuarterTable");
    private GeneralSource cleanedYearTable = new GeneralSource("CleanedYearTable");
    private GeneralSource depivotedMetrics = new GeneralSource("DepivotedMetrics");

    private GeneralSource source = depivotedMetrics;

    private String MAX_TXN_DATE = "2018-01-01";

    private ActivityMetrics weekMarginMetrics;
    private ActivityMetrics weekShareOfWalletMetrics;
    private ActivityMetrics weekSpendChangeMetrics;
    private ActivityMetrics weekAvgSpendOvertimeMetrics;
    private ActivityMetrics weekTotalSpendOvertimeMetrics;

    @Test(groups = "functional", enabled = true)
    public void testTransformation() {
        prepareAccount();
        prepareProduct();
        prepareWeekTable();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmIntermediateSource(cleanedWeekTable, null);
        confirmIntermediateSource(depivotedMetrics, null);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
    }

    @Override
    protected Source getSource() {
        return source;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("PurchaseMetricsCurator");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step00 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add(weekTable.getSourceName());
        baseSources.add(account.getSourceName());
        baseSources.add(product.getSourceName());
        step00.setBaseSources(baseSources);
        step00.setTransformer(DataCloudConstants.PURCHASE_METRICS_INITIATOR);
        step00.setTargetSource(cleanedWeekTable.getSourceName());
        step00.setConfiguration("{}");

        TransformationStepConfig step10 = new TransformationStepConfig();
        baseSources = new ArrayList<>();
        baseSources.add(cleanedWeekTable.getSourceName());
        step10.setBaseSources(baseSources);
        step10.setTransformer(DataCloudConstants.ACTIVITY_METRICS_CURATOR);
        step10.setTargetSource(depivotedMetrics.getSourceName());
        step10.setConfiguration(getActivityMetricsCuratorConfig());

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step00);
        steps.add(step10);

        // -----------
        configuration.setSteps(steps);
        configuration.setVersion(HdfsPathBuilder.dateFormat.format(new Date()));
        configuration.setKeepTemp(true);
        return configuration;
    }

    private String getActivityMetricsCuratorConfig() {
        ActivityMetricsCuratorConfig conf = new ActivityMetricsCuratorConfig();
        conf.setGroupByFields(Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.ProductId.name()));
        conf.setMaxTxnDate(MAX_TXN_DATE);
        weekMarginMetrics = new ActivityMetrics();
        weekMarginMetrics.setMetrics(InterfaceName.Margin);
        weekMarginMetrics.setPeriodsConfig(Arrays.asList(TimeFilter.within(1, Period.Week)));
        weekShareOfWalletMetrics = new ActivityMetrics();
        weekShareOfWalletMetrics.setMetrics(InterfaceName.ShareOfWallet);
        weekShareOfWalletMetrics.setPeriodsConfig(Arrays.asList(TimeFilter.within(1, Period.Week)));
        weekAvgSpendOvertimeMetrics = new ActivityMetrics();
        weekAvgSpendOvertimeMetrics.setMetrics(InterfaceName.AvgSpendOvertime);
        weekAvgSpendOvertimeMetrics.setPeriodsConfig(Arrays.asList(TimeFilter.within(1, Period.Week)));
        weekTotalSpendOvertimeMetrics = new ActivityMetrics();
        weekTotalSpendOvertimeMetrics.setMetrics(InterfaceName.TotalSpendOvertime);
        weekTotalSpendOvertimeMetrics.setPeriodsConfig(Arrays.asList(TimeFilter.within(1, Period.Week)));
        weekSpendChangeMetrics = new ActivityMetrics();
        weekSpendChangeMetrics.setMetrics(InterfaceName.SpendChange);
        weekSpendChangeMetrics.setPeriodsConfig(
                Arrays.asList(TimeFilter.within(1, Period.Week), TimeFilter.between(2, 2, Period.Week)));
        conf.setMetrics(Arrays.asList(weekMarginMetrics, weekShareOfWalletMetrics, weekAvgSpendOvertimeMetrics,
                weekTotalSpendOvertimeMetrics, weekSpendChangeMetrics));
        conf.setPeriodStrategies(Arrays.asList(PeriodStrategy.CalendarWeek));
        return JsonUtils.serialize(conf);
    }

    private void prepareAccount() {
        // Only put attrs which are needed in this test
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(InterfaceName.AccountId.name(), String.class));
        schema.add(Pair.of(InterfaceName.SpendAnalyticsSegment.name(), String.class));

        Object[][] data = new Object[][] { //
                { "AID1", "SEG1" }, //
                { "AID2", "SEG1" }, //
                { "AID3", "SEG3" }, //
                { "AID4", "SEG3" }, //
                { "AID5", "SEG5" }, //
                { "AID6", "SEG6" }, //
                { "AID7", null }, //
                { "AID8", "SEG8" }, //
        };

        uploadBaseSourceData(account.getSourceName(), baseSourceVersion, schema, data);
        try {
            extractSchema(account, baseSourceVersion,
                    hdfsPathBuilder.constructSnapshotDir(account.getSourceName(), baseSourceVersion).toString());
        } catch (Exception e) {
            log.error(String.format("Fail to extract schema for source %s at version %s", account.getSourceName(),
                    baseSourceVersion));
        }
    }

    private void prepareProduct() {
        // Only put attrs which are needed in this test
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(InterfaceName.ProductId.name(), String.class));
        schema.add(Pair.of(InterfaceName.ProductType.name(), String.class));

        Object[][] data = new Object[][] { //
                { "PID1", ProductType.ANALYTIC.name() }, //
                { "PID2", ProductType.ANALYTIC.name() }, //
                { "PID3", ProductType.ANALYTIC.name() }, //
                { "PID4", ProductType.ANALYTIC.name() }, //
                { "PID5", ProductType.SPENDING.name() }, //
                { "PID6", null }, //
        };

        uploadBaseSourceData(product.getSourceName(), baseSourceVersion, schema, data);
        try {
            extractSchema(product, baseSourceVersion,
                    hdfsPathBuilder.constructSnapshotDir(product.getSourceName(), baseSourceVersion).toString());
        } catch (Exception e) {
            log.error(String.format("Fail to extract schema for source %s at version %s", product.getSourceName(),
                    baseSourceVersion));
        }
    }


    /*
     * max txn date
     * 2018-01-01: week = 940, month = 216, quarter = 72, year = 18
     * 
     * last week, last month, last quarter, last year
     * 2017-12-29: week = 939, month = 215, quarter = 71, year = 17
     * 
     * last 2 weeks, last month, last quarter, last year
     * 2017-12-14: week = 938, month = 215, quarter = 71, year = 17
     * 
     * last 2 months, last quarter, last year
     * 2017-11-30: week = 935, month = 214, quarter = 71, year = 17
     * 
     * last 2 quarters, last year
     * 2017-05-31: week = 909, month = 208, quarter = 69, year = 17
     * 
     * last 2 years
     * 2016-12-30: week = 887, month = 203, quarter = 69, year = 16
     */

    
    // Test case:
    // AID1: Cover all products, data all valid, filter non-analytic product
    // AID2: Miss some product, data all valid
    // AID3: Miss some product, some amount is empty or 0
    // AID4: Miss some product, some cost is empty or 0
    // AID5: Miss some product, no any data valid
    // AID6: Miss some product, has some product not existing in Product table, miss prior 2 week data
    // AID7: Miss segment
    // AID8: Miss last week data

    // Schema: TransactionId, AccountId, ProductId, TotalAmount, TotalCost, PeriodName, PeriodId
    private Object[][] weekData = new Object[][] {
            // max txn date
            { "TID001", "AID1", "PID1", 200.0, 100.0, "Week",
                    PeriodBuilderFactory.build(PeriodStrategy.CalendarWeek).toPeriodId(MAX_TXN_DATE) }, //

            /**** last week ****/
            { "TID101", "AID1", "PID1", 20.0, 15.0, "Week", 939 }, //
            { "TID102", "AID1", "PID1", 25.0, 15.0, "Week", 939 }, //
            { "TID103", "AID1", "PID2", 20.0, 15.0, "Week", 939 }, //
            { "TID104", "AID1", "PID2", 25.0, 15.0, "Week", 939 }, //
            { "TID105", "AID1", "PID3", 20.0, 15.0, "Week", 939 }, //
            { "TID106", "AID1", "PID3", 25.0, 15.0, "Week", 939 }, //
            { "TID107", "AID1", "PID4", 20.0, 15.0, "Week", 939 }, //
            { "TID108", "AID1", "PID4", 25.0, 15.0, "Week", 939 }, //
            { "TID109", "AID1", "PID5", 20.0, 15.0, "Week", 939 }, // non-analytic product
            { "TID110", "AID1", "PID5", 25.0, 15.0, "Week", 939 }, // non-analytic product
            { "TID111", "AID1", "PID6", 20.0, 15.0, "Week", 939 }, // no-type product
            { "TID112", "AID1", "PID6", 25.0, 15.0, "Week", 939 }, // no-type product

            { "TID113", "AID2", "PID1", 20.0, 15.0, "Week", 939 }, //
            { "TID114", "AID2", "PID2", 20.0, 15.0, "Week", 939 }, //

            { "TID115", "AID3", "PID1", 20.0, 15.0, "Week", 939 }, //
            { "TID116", "AID3", "PID1", null, 15.0, "Week", 939 }, // null amount
            { "TID117", "AID3", "PID1", 0.0, 15.0, "Week", 939 }, // 0 amount
            { "TID118", "AID3", "PID2", null, 15.0, "Week", 939 }, // null amount
            { "TID119", "AID3", "PID3", 0.0, 15.0, "Week", 939 }, // 0 amount

            { "TID120", "AID4", "PID1", 20.0, 15.0, "Week", 939 }, //
            { "TID121", "AID4", "PID1", 20.0, null, "Week", 939 }, // null cost
            { "TID122", "AID4", "PID1", 20.0, 0.0, "Week", 939 }, // 0 cost
            { "TID123", "AID4", "PID2", 20.0, null, "Week", 939 }, // null cost
            { "TID124", "AID4", "PID3", 20.0, 0.0, "Week", 939 }, // 0 cost

            { "TID125", "AID5", "PID1", null, null, "Week", 939 }, // null amount, null cost
            { "TID126", "AID5", "PID1", null, 0.0, "Week", 939 }, // null amount, 0 cost
            { "TID127", "AID5", "PID2", 0.0, null, "Week", 939 }, // 0 amount, null cost
            { "TID128", "AID5", "PID2", 0.0, 0.0, "Week", 939 }, // 0 amount, 0 cost

            { "TID129", "AID6", "PID1", 10.0, 5.0, "Week", 939 }, //
            { "TID130", "AID6", "PID2", 10.0, 5.0, "Week", 939 }, //
            { "TID131", "AID6", "PIDNotExist", 10.0, 5.0, "Week", 939 }, // non-existing product
            { "TID132", "AIDNotExist", "PIDNotExist", 10.0, 5.0, "Week", 939 }, // non-existing account
            
            { "TID133", "AID7", "PID1", 10.0, 5.0, "Week", 939 }, // no-segment account

            // No AID8 for last week


            /**** prior 2 week (to test spend change) ****/
            // AID1 has spend change on each product
            { "TID201", "AID1", "PID1", 20.0, 15.0, "Week", 938 }, //
            { "TID202", "AID1", "PID2", 20.0, 15.0, "Week", 938 }, //
            { "TID203", "AID1", "PID3", 20.0, 15.0, "Week", 938 }, //
            { "TID204", "AID1", "PID4", 20.0, 15.0, "Week", 938 }, //
            
            // AID2 last week: PID1, PID2; prior 2 week: PID2, PID3
            { "TID205", "AID2", "PID2", 20.0, 15.0, "Week", 938 }, //
            { "TID206", "AID2", "PID3", 20.0, 15.0, "Week", 938 }, //
            
            // AID3 no change; amounts are patial null or 0
            { "TID207", "AID3", "PID1", 20.0, 15.0, "Week", 938 }, //
            { "TID208", "AID3", "PID1", null, 15.0, "Week", 938 }, // null amount
            { "TID209", "AID3", "PID1", 0.0, 15.0, "Week", 938 }, // 0 amount
            { "TID210", "AID3", "PID2", null, 15.0, "Week", 938 }, // null amount
            { "TID211", "AID3", "PID3", 0.0, 15.0, "Week", 938 }, // 0 amount
            
            // AID5 no change; amounts are all null or 0
            { "TID217", "AID5", "PID1", null, null, "Week", 938 }, // null amount, null cost
            { "TID218", "AID5", "PID1", null, 0.0, "Week", 938 }, // null amount, 0 cost
            { "TID219", "AID5", "PID2", 0.0, null, "Week", 938 }, // 0 amount, null cost
            { "TID220", "AID5", "PID2", 0.0, 0.0, "Week", 938 }, // 0 amount, 0 cost
            
            // No AID4, AID6 for prior 2 week

            // AID8 only exists in prior 2 week
            { "TID221", "AID8", "PID1", 20.0, 15.0, "Week", 938 }, //

    };

    // Schema: AccountId, ProductId, Week1_Margin, Week1_ShareOfWallet, Week1_AvgSpendOvertime, Week1_TotalSpendOvertime, Week1_Week2_2_SpendChange
    private Object[][] depivotedMetricsData = new Object[][] {
            { "AID5", "PID1", null, null, null, null, 0 }, //
            { "AID3", "PID3", null, null, null, null, 0 }, //
            { "AID4", "PID1", 300, 90, 20.0, 60.0, 100 }, //
            { "AID3", "PID2", null, null, null, null, 0 }, //
            { "AID2", "PID3", null, null, null, null, -100 }, //
            { "AID3", "PID1", -56, 150, 6.666666666666667, 20.0, 0 }, //
            { "AID5", "PID2", null, null, null, null, 0 }, //
            { "AID6", "PID2", 100, 100, 10.0, 10.0, 100 }, //
            { "AID6", "PID1", 100, 100, 10.0, 10.0, 100 }, //
            { "AID8", "PID1", null, null, null, null, -100 }, //
            { "AID1", "PID1", 50, 85, 22.5, 45.0, 13 }, //
            { "AID1", "PID2", 50, 85, 22.5, 45.0, 13 }, //
            { "AID1", "PID4", 50, 122, 22.5, 45.0, 13 }, //
            { "AID4", "PID2", null, 120, 20.0, 20.0, 100 }, //
            { "AID4", "PID3", null, 120, 20.0, 20.0, 100 }, //
            { "AID1", "PID3", 50, 122, 22.5, 45.0, 13 }, //
            { "AID2", "PID1", 33, 169, 20.0, 20.0, 100 }, //
            { "AID2", "PID2", 33, 169, 20.0, 20.0, 0 },//
    };

    private void prepareWeekTable() {
        // Only put attrs which are needed in this test. Faked period table
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(InterfaceName.TransactionId.name(), String.class));
        schema.add(Pair.of(InterfaceName.AccountId.name(), String.class));
        schema.add(Pair.of(InterfaceName.ProductId.name(), String.class));
        schema.add(Pair.of(InterfaceName.TotalAmount.name(), Double.class));
        schema.add(Pair.of(InterfaceName.TotalCost.name(), Double.class));
        schema.add(Pair.of(InterfaceName.PeriodName.name(), String.class));
        schema.add(Pair.of(InterfaceName.PeriodId.name(), Integer.class));

        uploadBaseSourceData(weekTable.getSourceName(), baseSourceVersion, schema, weekData);
        try {
            extractSchema(weekTable, baseSourceVersion,
                    hdfsPathBuilder.constructSnapshotDir(weekTable.getSourceName(), baseSourceVersion).toString());
        } catch (Exception e) {
            log.error(String.format("Fail to extract schema for source %s at version %s", weekTable.getSourceName(),
                    baseSourceVersion));
        }
    }

    @Override
    protected void verifyIntermediateResult(String source, String version, Iterator<GenericRecord> records) {
        log.info(String.format("Start to verify intermediate source %s", source));
        try {
            switch (source) {
            case "CleanedWeekTable":
                verifyCleanedWeekTable(records);
                break;
            case "DepivotedMetrics":
                verifyDepivotedMetrics(records);
                break;
            default:
                throw new UnsupportedOperationException(String.format("Unknown intermediate source %s", source));
            }
        } catch (Exception ex) {
            throw new RuntimeException("Exception in verifyIntermediateResult", ex);
        }
    }

    private void verifyCleanedWeekTable(Iterator<GenericRecord> records) {
        log.info("Verifying cleaned week table");
        int cnt = 0;
        while (records.hasNext()) {
            log.info(records.next().toString());
            cnt++;
        }
        Assert.assertEquals(cnt, weekData.length - 7);
    }

    private void verifyDepivotedMetrics(Iterator<GenericRecord> records) {
        log.info("Verifying depivoted metrics");
        Map<String, Object[]> expectedMetrics = new HashMap<>();
        for (Object[] ent : depivotedMetricsData) {
            expectedMetrics.put(ent[0].toString() + ent[1].toString(), ent);    // AccountId + ProductId is unique
        }

        int cnt = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            String key = record.get(InterfaceName.AccountId.name()).toString()
                    + record.get(InterfaceName.ProductId.name()).toString();
            Object[] expected = expectedMetrics.get(key);
            Assert.assertNotNull(expected);
            Assert.assertTrue(isObjEquals(record.get(weekMarginMetrics.getFullMetricsName()), expected[2]));
            Assert.assertTrue(isObjEquals(record.get(weekShareOfWalletMetrics.getFullMetricsName()), expected[3]));
            Assert.assertTrue(isObjEquals(record.get(weekAvgSpendOvertimeMetrics.getFullMetricsName()), expected[4]));
            Assert.assertTrue(isObjEquals(record.get(weekTotalSpendOvertimeMetrics.getFullMetricsName()), expected[5]));
            Assert.assertTrue(isObjEquals(record.get(weekSpendChangeMetrics.getFullMetricsName()), expected[6]));
            cnt++;
        }
        Assert.assertEquals(cnt, 18);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
    }

}
