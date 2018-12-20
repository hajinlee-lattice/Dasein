package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ActivityMetricsPivotConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.domain.exposed.util.ActivityMetricsUtils;

public class PurchaseMetricsCuratorTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(PurchaseMetricsCuratorTestNG.class);

    private GeneralSource account = new GeneralSource("Account");
    private GeneralSource accountNoSegment = new GeneralSource("AccountNoSegment");
    private GeneralSource product = new GeneralSource("Product");
    private GeneralSource weekTable = new GeneralSource("WeekTable");
    private GeneralSource depivotedMetrics = new GeneralSource("DepivotedMetrics");
    private GeneralSource pivotMetrics = new GeneralSource("PivotMetrics");
    private GeneralSource reducedDepivotedMetrics = new GeneralSource("ReducedDepivotedMetrics");
    private GeneralSource expandedPivotMetrics = new GeneralSource("ExpandedPivotMetrics");
    private GeneralSource depivotedMetricsNoSegment = new GeneralSource("DepivotedMetricsNoSegment");
    private GeneralSource loadTest = new GeneralSource("LoadTestMetrics");

    private GeneralSource source = loadTest;

    private String MAX_TXN_DATE = "2018-01-01";

    private ActivityMetrics weekMG;
    private ActivityMetrics weekSW;
    private ActivityMetrics weekSWDepr;
    private ActivityMetrics weekSC;
    private ActivityMetrics weekASDepr;
    private ActivityMetrics weekAS1; // last 1 week
    private ActivityMetrics weekAS2; // last 1-2 weeks
    private ActivityMetrics weekTSDepr;
    private ActivityMetrics weekTS1; // last 1 week
    private ActivityMetrics weekTS2; // last 1-2 weeks
    private ActivityMetrics everHP;

    private List<ActivityMetrics> metricsList;
    private List<ActivityMetrics> metricsListLoadTest;

    private String AID_NO_TXN = "AID_NO_TXN";

    @Test(groups = "functional")
    public void testTransformation() {
        prepareMetrics();
        prepareAccount();
        prepareProduct();
        prepareWeekTable();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmIntermediateSource(depivotedMetrics, null);
        confirmIntermediateSource(pivotMetrics, null);
        confirmIntermediateSource(reducedDepivotedMetrics, null);
        confirmIntermediateSource(expandedPivotMetrics, null);
        confirmIntermediateSource(depivotedMetricsNoSegment, null);
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

        TransformationStepConfig step10 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add(weekTable.getSourceName());
        baseSources.add(account.getSourceName());
        baseSources.add(product.getSourceName());
        step10.setBaseSources(baseSources);
        step10.setTransformer(DataCloudConstants.ACTIVITY_METRICS_CURATOR);
        step10.setTargetSource(depivotedMetrics.getSourceName());
        step10.setConfiguration(getActivityMetricsCuratorConfig(metricsList, false, true));

        TransformationStepConfig step20 = new TransformationStepConfig();
        baseSources = new ArrayList<>();
        baseSources.add(depivotedMetrics.getSourceName());
        step20.setBaseSources(baseSources);
        step20.setTransformer(DataCloudConstants.ACTIVITY_METRICS_PIVOT);
        step20.setTargetSource(pivotMetrics.getSourceName());
        step20.setConfiguration(getActivityMetricsPivotConfig(metricsList, false));

        TransformationStepConfig step30 = new TransformationStepConfig();
        baseSources = new ArrayList<>();
        baseSources.add(weekTable.getSourceName());
        baseSources.add(account.getSourceName());
        baseSources.add(product.getSourceName());
        step30.setBaseSources(baseSources);
        step30.setTransformer(DataCloudConstants.ACTIVITY_METRICS_CURATOR);
        step30.setTargetSource(reducedDepivotedMetrics.getSourceName());
        step30.setConfiguration(getActivityMetricsCuratorConfig(metricsList, true, true));

        TransformationStepConfig step40 = new TransformationStepConfig();
        baseSources = new ArrayList<>();
        baseSources.add(reducedDepivotedMetrics.getSourceName());
        baseSources.add(account.getSourceName());
        step40.setBaseSources(baseSources);
        step40.setTransformer(DataCloudConstants.ACTIVITY_METRICS_PIVOT);
        step40.setTargetSource(expandedPivotMetrics.getSourceName());
        step40.setConfiguration(getActivityMetricsPivotConfig(metricsList, true));

        TransformationStepConfig step50 = new TransformationStepConfig();
        baseSources = new ArrayList<>();
        baseSources.add(weekTable.getSourceName());
        baseSources.add(accountNoSegment.getSourceName());
        baseSources.add(product.getSourceName());
        step50.setBaseSources(baseSources);
        step50.setTransformer(DataCloudConstants.ACTIVITY_METRICS_CURATOR);
        step50.setTargetSource(depivotedMetricsNoSegment.getSourceName());
        step50.setConfiguration(getActivityMetricsCuratorConfig(metricsList, true, false));

        TransformationStepConfig step60 = new TransformationStepConfig();
        baseSources = new ArrayList<>();
        baseSources.add(weekTable.getSourceName());
        baseSources.add(account.getSourceName());
        baseSources.add(product.getSourceName());
        step60.setBaseSources(baseSources);
        step60.setTransformer(DataCloudConstants.ACTIVITY_METRICS_CURATOR);
        step60.setConfiguration(getActivityMetricsCuratorConfig(metricsListLoadTest, true, true));

        TransformationStepConfig step70 = new TransformationStepConfig();
        List<Integer> inputSteps = Arrays.asList(5);
        step70.setInputSteps(inputSteps);
        baseSources = new ArrayList<>();
        baseSources.add(account.getSourceName());
        step70.setBaseSources(baseSources);
        step70.setTransformer(DataCloudConstants.ACTIVITY_METRICS_PIVOT);
        step70.setTargetSource(loadTest.getSourceName());
        step70.setConfiguration(getActivityMetricsPivotConfig(metricsListLoadTest, true));

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step10);
        steps.add(step20);
        steps.add(step30);
        steps.add(step40);
        steps.add(step50);
        steps.add(step60);
        steps.add(step70);

        // -----------
        configuration.setSteps(steps);
        configuration.setVersion(HdfsPathBuilder.dateFormat.format(new Date()));
        configuration.setKeepTemp(true);
        return configuration;
    }

    private void prepareMetrics() {
        weekMG = new ActivityMetrics();
        weekMG.setMetrics(InterfaceName.Margin);
        weekMG.setPeriodsConfig(Arrays.asList(TimeFilter.within(1, PeriodStrategy.Template.Week.name())));

        weekSW = new ActivityMetrics();
        weekSW.setMetrics(InterfaceName.ShareOfWallet);
        weekSW.setPeriodsConfig(Arrays.asList(TimeFilter.within(1, PeriodStrategy.Template.Week.name())));

        weekSWDepr = new ActivityMetrics();
        weekSWDepr.setMetrics(InterfaceName.ShareOfWallet);
        weekSWDepr.setPeriodsConfig(Arrays.asList(TimeFilter.within(2, PeriodStrategy.Template.Week.name())));
        weekSWDepr.setEOL(true);

        // Deprecated AvgSpendOvertime with WITHIN relation (before M25)
        weekASDepr = new ActivityMetrics();
        weekASDepr.setMetrics(InterfaceName.AvgSpendOvertime);
        weekASDepr.setPeriodsConfig(Arrays.asList(TimeFilter.within(1, PeriodStrategy.Template.Week.name())));

        // AvgSpendOvertime with BETWEEN relation (after M25)
        weekAS1 = new ActivityMetrics();
        weekAS1.setMetrics(InterfaceName.AvgSpendOvertime);
        weekAS1.setPeriodsConfig(Arrays.asList(TimeFilter.between(1, 1, PeriodStrategy.Template.Week.name())));

        weekAS2 = new ActivityMetrics();
        weekAS2.setMetrics(InterfaceName.AvgSpendOvertime);
        weekAS2.setPeriodsConfig(Arrays.asList(TimeFilter.between(1, 2, PeriodStrategy.Template.Week.name())));

        // Deprecated TotalSpendOvertime with WITHIN relation (before M25)
        weekTSDepr = new ActivityMetrics();
        weekTSDepr.setMetrics(InterfaceName.TotalSpendOvertime);
        weekTSDepr.setPeriodsConfig(Arrays.asList(TimeFilter.within(1, PeriodStrategy.Template.Week.name())));

        // TotalSpendOvertime with BETWEEN relation (after M25)
        weekTS1 = new ActivityMetrics();
        weekTS1.setMetrics(InterfaceName.TotalSpendOvertime);
        weekTS1.setPeriodsConfig(Arrays.asList(TimeFilter.between(1, 1, PeriodStrategy.Template.Week.name())));

        weekTS2 = new ActivityMetrics();
        weekTS2.setMetrics(InterfaceName.TotalSpendOvertime);
        weekTS2.setPeriodsConfig(Arrays.asList(TimeFilter.between(1, 2, PeriodStrategy.Template.Week.name())));

        weekSC = new ActivityMetrics();
        weekSC.setMetrics(InterfaceName.SpendChange);
        weekSC.setPeriodsConfig(
                Arrays.asList(TimeFilter.within(1, PeriodStrategy.Template.Week.name()),
                        TimeFilter.between(2, 2, PeriodStrategy.Template.Week.name())));
        everHP = new ActivityMetrics();
        everHP.setMetrics(InterfaceName.HasPurchased);
        everHP.setPeriodsConfig(Arrays.asList(TimeFilter.ever(PeriodStrategy.Template.Week.name())));
        metricsList = Arrays.asList(weekMG, weekSW, weekSWDepr, weekASDepr,
                weekAS1, weekAS2, weekTSDepr, weekTS1, weekTS2, weekSC, everHP);

        metricsListLoadTest = new ArrayList<>();
        for (int i = 1; i <= 500; i++) {
            ActivityMetrics metrics = new ActivityMetrics();
            metrics.setMetrics(InterfaceName.TotalSpendOvertime);
            metrics.setPeriodsConfig(Arrays.asList(TimeFilter.between(i, i + 1, PeriodStrategy.Template.Week.name())));
            metrics.setEOL(true);
            metricsListLoadTest.add(metrics);
        }
    }

    private String getActivityMetricsCuratorConfig(List<ActivityMetrics> metricsList, boolean reduced,
            boolean accountHasSegment) {
        ActivityMetricsCuratorConfig conf = new ActivityMetricsCuratorConfig();
        conf.setGroupByFields(Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.ProductId.name()));
        conf.setCurrentDate(MAX_TXN_DATE);

        conf.setMetrics(metricsList);
        conf.setPeriodStrategies(Arrays.asList(PeriodStrategy.CalendarWeek));
        conf.setType(ActivityType.PurchaseHistory);
        conf.setReduced(reduced);
        conf.setAccountHasSegment(accountHasSegment);
        conf.setPeriodTableCnt(1);
        if (metricsList.size() > 500) {
            return setDataFlowEngine(JsonUtils.serialize(conf), "TEZ");
        } else {
            return JsonUtils.serialize(conf);
        }
    }

    private String getActivityMetricsPivotConfig(List<ActivityMetrics> metricsList, boolean expanded) {
        ActivityMetricsPivotConfig config = new ActivityMetricsPivotConfig();
        config.setActivityType(ActivityType.PurchaseHistory);
        config.setGroupByField(InterfaceName.AccountId.name());
        config.setPivotField(InterfaceName.ProductId.name());
        Map<String, List<Product>> productMap = new HashMap<>();
        productMap.put("PID1", null);
        productMap.put("PID2", null);
        productMap.put("PID3", null);
        productMap.put("PID4", null);
        config.setProductMap(productMap);
        config.setExpanded(expanded);
        if (config.isExpanded()) {
            config.setMetrics(metricsList);
        }
        if (metricsList.size() > 500) {
            return setDataFlowEngine(JsonUtils.serialize(config), "TEZ");
        } else {
            return JsonUtils.serialize(config);
        }
    }

    private Object[][] accountData = new Object[][] { //
            { "AID1", "SEG1" }, //
            { "AID2", "SEG1" }, //
            { "AID3", "SEG3" }, //
            { "AID4", "SEG3" }, //
            { "AID5", "SEG5" }, //
            { "AID6", "SEG6" }, //
            { "AID7", null }, //
            { "AID8", "SEG8" }, //
            { AID_NO_TXN, "SEG_NO_TXN" }, // AID not exists in Transaction
    };

    private Object[][] accountDataNoSegment = new Object[][] { //
            { "AID1" }, //
            { "AID2" }, //
    };

    private void prepareAccount() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(InterfaceName.AccountId.name(), String.class));
        schema.add(Pair.of(InterfaceName.SpendAnalyticsSegment.name(), String.class));

        uploadBaseSourceData(account.getSourceName(), baseSourceVersion, schema, accountData);
        try {
            extractSchema(account, baseSourceVersion,
                    hdfsPathBuilder.constructSnapshotDir(account.getSourceName(), baseSourceVersion).toString());
        } catch (Exception e) {
            log.error(String.format("Fail to extract schema for source %s at version %s", account.getSourceName(),
                    baseSourceVersion));
        }

        List<Pair<String, Class<?>>> schemaNoSegment = new ArrayList<>();
        schema.add(Pair.of(InterfaceName.AccountId.name(), String.class));

        uploadBaseSourceData(accountNoSegment.getSourceName(), baseSourceVersion, schemaNoSegment,
                accountDataNoSegment);
        try {
            extractSchema(accountNoSegment, baseSourceVersion, hdfsPathBuilder
                    .constructSnapshotDir(accountNoSegment.getSourceName(), baseSourceVersion).toString());
        } catch (Exception e) {
            log.error(String.format("Fail to extract schema for source %s at version %s",
                    accountNoSegment.getSourceName(), baseSourceVersion));
        }
    }

    private Object[][] productData = new Object[][] { //
            { "PID1", ProductType.Analytic.name() }, //
            { "PID2", ProductType.Analytic.name() }, //
            { "PID3", ProductType.Analytic.name() }, //
            { "PID4", ProductType.Analytic.name() }, //
            { "PID5", ProductType.Spending.name() }, //
            { "PID6", null }, //
    };

    private void prepareProduct() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(InterfaceName.ProductId.name(), String.class));
        schema.add(Pair.of(InterfaceName.ProductType.name(), String.class));

        uploadBaseSourceData(product.getSourceName(), baseSourceVersion, schema, productData);
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
    private int maxWeek = PeriodBuilderFactory.build(PeriodStrategy.CalendarWeek).toPeriodId(MAX_TXN_DATE);
    private Object[][] weekData = new Object[][] {
            // max txn date
            { "TID001", "AID1", "PID1", 200.0, 100.0, "Week", maxWeek }, //

            /**** last week ****/
            { "TID101", "AID1", "PID1", 20.0, 15.0, "Week", maxWeek - 1 }, //
            { "TID102", "AID1", "PID1", 25.0, 15.0, "Week", maxWeek - 1 }, //
            { "TID103", "AID1", "PID2", 20.0, 15.0, "Week", maxWeek - 1 }, //
            { "TID104", "AID1", "PID2", 25.0, 15.0, "Week", maxWeek - 1 }, //
            { "TID105", "AID1", "PID3", 20.0, 15.0, "Week", maxWeek - 1 }, //
            { "TID106", "AID1", "PID3", 25.0, 15.0, "Week", maxWeek - 1 }, //
            { "TID107", "AID1", "PID4", 20.0, 15.0, "Week", maxWeek - 1 }, //
            { "TID108", "AID1", "PID4", 25.0, 15.0, "Week", maxWeek - 1 }, //
            { "TID109", "AID1", "PID5", 20.0, 15.0, "Week", maxWeek - 1 }, // non-analytic product
            { "TID110", "AID1", "PID5", 25.0, 15.0, "Week", maxWeek - 1 }, // non-analytic product
            { "TID111", "AID1", "PID6", 20.0, 15.0, "Week", maxWeek - 1 }, // no-type product
            { "TID112", "AID1", "PID6", 25.0, 15.0, "Week", maxWeek - 1 }, // no-type product

            { "TID113", "AID2", "PID1", 20.0, 15.0, "Week", maxWeek - 1 }, //
            { "TID114", "AID2", "PID2", 20.0, 15.0, "Week", maxWeek - 1 }, //

            { "TID115", "AID3", "PID1", 20.0, 15.0, "Week", maxWeek - 1 }, //
            { "TID116", "AID3", "PID1", null, 15.0, "Week", maxWeek - 1 }, // null amount
            { "TID117", "AID3", "PID1", 0.0, 15.0, "Week", maxWeek - 1 }, // 0 amount
            { "TID118", "AID3", "PID2", null, 15.0, "Week", maxWeek - 1 }, // null amount
            { "TID119", "AID3", "PID3", 0.0, 15.0, "Week", maxWeek - 1 }, // 0 amount

            { "TID120", "AID4", "PID1", 20.0, 15.0, "Week", maxWeek - 1 }, //
            { "TID121", "AID4", "PID1", 20.0, null, "Week", maxWeek - 1 }, // null cost
            { "TID122", "AID4", "PID1", 20.0, 0.0, "Week", maxWeek - 1 }, // 0 cost
            { "TID123", "AID4", "PID2", 20.0, null, "Week", maxWeek - 1 }, // null cost
            { "TID124", "AID4", "PID3", 20.0, 0.0, "Week", maxWeek - 1 }, // 0 cost

            { "TID125", "AID5", "PID1", null, null, "Week", maxWeek - 1 }, // null amount, null cost
            { "TID126", "AID5", "PID1", null, 0.0, "Week", maxWeek - 1 }, // null amount, 0 cost
            { "TID127", "AID5", "PID2", 0.0, null, "Week", maxWeek - 1 }, // 0 amount, null cost
            { "TID128", "AID5", "PID2", 0.0, 0.0, "Week", maxWeek - 1 }, // 0 amount, 0 cost

            { "TID129", "AID6", "PID1", 10.0, 5.0, "Week", maxWeek - 1 }, //
            { "TID130", "AID6", "PID2", 10.0, 5.0, "Week", maxWeek - 1 }, //
            { "TID131", "AID6", "PIDNotExist", 10.0, 5.0, "Week", maxWeek - 1 }, // non-existing product
            { "TID132", "AIDNotExist", "PIDNotExist", 10.0, 5.0, "Week", maxWeek - 1 }, // non-existing account
            
            { "TID133", "AID7", "PID1", 10.0, 5.0, "Week", maxWeek - 1 }, // no-segment account

            // No AID8 for last week


            /**** prior 2 week (to test spend change) ****/
            // AID1 has spend change on each product
            { "TID201", "AID1", "PID1", 20.0, 15.0, "Week", maxWeek - 2 }, //
            { "TID202", "AID1", "PID2", 20.0, 15.0, "Week", maxWeek - 2 }, //
            { "TID203", "AID1", "PID3", 20.0, 15.0, "Week", maxWeek - 2 }, //
            { "TID204", "AID1", "PID4", 20.0, 15.0, "Week", maxWeek - 2 }, //
            
            // AID2 last week: PID1, PID2; prior 2 week: PID2, PID3
            { "TID205", "AID2", "PID2", 20.0, 15.0, "Week", maxWeek - 2 }, //
            { "TID206", "AID2", "PID3", 20.0, 15.0, "Week", maxWeek - 2 }, //
            
            // AID3 no change; amounts are patial null or 0
            { "TID207", "AID3", "PID1", 20.0, 15.0, "Week", maxWeek - 2 }, //
            { "TID208", "AID3", "PID1", null, 15.0, "Week", maxWeek - 2 }, // null amount
            { "TID209", "AID3", "PID1", 0.0, 15.0, "Week", maxWeek - 2 }, // 0 amount
            { "TID210", "AID3", "PID2", null, 15.0, "Week", maxWeek - 2 }, // null amount
            { "TID211", "AID3", "PID3", 0.0, 15.0, "Week", maxWeek - 2 }, // 0 amount
            
            // AID5 no change; amounts are all null or 0
            { "TID217", "AID5", "PID1", null, null, "Week", maxWeek - 2 }, // null amount, null cost
            { "TID218", "AID5", "PID1", null, 0.0, "Week", maxWeek - 2 }, // null amount, 0 cost
            { "TID219", "AID5", "PID2", 0.0, null, "Week", maxWeek - 2 }, // 0 amount, null cost
            { "TID220", "AID5", "PID2", 0.0, 0.0, "Week", maxWeek - 2 }, // 0 amount, 0 cost
            
            // No AID4, AID6 for prior 2 week

            // AID8 only exists in prior 2 week
            { "TID221", "AID8", "PID1", 20.0, 15.0, "Week", maxWeek - 2 }, //

    };

    // Schema: AccountId, ProductId, W_1__MG, W_1__SW, W_2__SW, W_1__AS,
    // W_1_1__AS, W_1_2__AS, W_1__TS, W_1_2__TS,W_1_1__TS, W_1__W_2_2__SC,
    // EVER__HP
    private Object[][] depivotedMetricsData = new Object[][] { //
            { "AID1", "PID1", 33, 85, 100, 45.0, 45.0, 32.5, 45.0, 45.0, 65.0, 125, true }, //
            { "AID1", "PID2", 33, 85, 81, 45.0, 45.0, 32.5, 45.0, 45.0, 65.0, 125, true }, //
            { "AID1", "PID3", 33, 122, 100, 45.0, 45.0, 32.5, 45.0, 45.0, 65.0, 125, true }, //
            { "AID1", "PID4", 33, 122, 131, 45.0, 45.0, 32.5, 45.0, 45.0, 65.0, 125, true }, //

            { "AID2", "PID1", 25, 169, 100, 20.0, 20.0, 10.0, 20.0, 20.0, 20.0, 100, true }, //
            { "AID2", "PID2", 25, 169, 162, 20.0, 20.0, 20.0, 20.0, 20.0, 40.0, 0, true }, //
            { "AID2", "PID3", null, 0, 100, 0.0, 0.0, 10.0, 0.0, 0.0, 20.0, -100, true }, //
            { "AID2", "PID4", null, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false }, //

            { "AID3", "PID1", -125, 150, 140, 20.0, 20.0, 20.0, 20.0, 20.0, 40.0, 0, true }, //
            { "AID3", "PID2", null, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, true }, //
            { "AID3", "PID3", null, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, true }, //
            { "AID3", "PID4", null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false }, //

            { "AID4", "PID1", 75, 90, 84, 60.0, 60.0, 30.0, 60.0, 60.0, 60.0, 100, true }, //
            { "AID4", "PID2", null, 120, 140, 20.0, 20.0, 10.0, 20.0, 20.0, 20.0, 100, true }, //
            { "AID4", "PID3", null, 120, 140, 20.0, 20.0, 10.0, 20.0, 20.0, 20.0, 100, true }, //
            { "AID4", "PID4", null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false }, //

            { "AID5", "PID1", null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, true }, //
            { "AID5", "PID2", null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, true }, //
            { "AID5", "PID3", null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false }, //
            { "AID5", "PID4", null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false }, //

            { "AID6", "PID1", 50, 100, 100, 10.0, 10.0, 5.0, 10.0, 10.0, 10.0, 100, true }, //
            { "AID6", "PID2", 50, 100, 100, 10.0, 10.0, 5.0, 10.0, 10.0, 10.0, 100, true }, //
            { "AID6", "PID3", null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false }, //
            { "AID6", "PID4", null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false }, //

            { "AID7", "PID1", 50, null, null, 10.0, 10.0, 5.0, 10.0, 10.0, 10.0, 100, true }, //
            { "AID7", "PID2", null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false }, //
            { "AID7", "PID3", null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false }, //
            { "AID7", "PID4", null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false }, //

            { "AID8", "PID1", null, null, 100, 0.0, 0.0, 10.0, 0.0, 0.0, 20.0, -100, true }, //
            { "AID8", "PID2", null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false }, //
            { "AID8", "PID3", null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false }, //
            { "AID8", "PID4", null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false }, //

            { AID_NO_TXN, "PID1", null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false }, //
            { AID_NO_TXN, "PID2", null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false }, //
            { AID_NO_TXN, "PID3", null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false }, //
            { AID_NO_TXN, "PID4", null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false }, //
    };

    // Schema: AccountId
    // AM_PID1__W_1__MG, AM_PID1__W_1__SW, AM_PID2__W_2__SW, AM_PID1__W_1__AS,
    // AM_PID1__W_1_1__AS, AM_PID1__W_1_2__AS, AM_PID1__W_1__TS,
    // AM_PID1__W_1_1__TS, AM_PID1__W_1_2__TS, AM_PID1__W_1__W_2_2__SC,
    // AM_PID1__EVER__HP

    // AM_PID2__W_1__MG, AM_PID2__W_1__SW, AM_PID2__W_2__SW, AM_PID2__W_1__AS,
    // AM_PID2__W_1_1__AS, AM_PID2__W_1_2__AS, AM_PID2__W_1__TS,
    // AM_PID2__W_1_1__TS, AM_PID2__W_1_2__TS, AM_PID2__W_1__W_2_2__SC,
    // AM_PID2__EVER__HP

    // AM_PID3__W_1__MG, AM_PID3__W_1__SW, AM_PID3__W_2__SW, AM_PID3__W_1__AS,
    // AM_PID3__W_1_1__AS, AM_PID3__W_1_2__AS, AM_PID3__W_1__TS,
    // AM_PID3__W_1_1__TS, AM_PID3__W_1_2__TS, AM_PID3__W_1__W_2_2__SC,
    // AM_PID3__EVER__HP

    // AM_PID4__W_1__MG, AM_PID4__W_1__SW, AM_PID4__W_2__SW, AM_PID4__W_1__AS,
    // AM_PID4__W_1_1__AS, AM_PID4__W_1_2__AS, AM_PID4__W_1__TS,
    // AM_PID4__W_1_1__TS, AM_PID4__W_1_2__TS, AM_PID4__W_1__W_2_2__SC,
    // AM_PID4__EVER__HP
    private Object[][] pivotMetricsData = new Object[][] {
            { "AID1", //
                    33, 85, 100, 45.0, 45.0, 32.5, 45.0, 45.0, 65.0, 125, true, //
                    33, 85, 81, 45.0, 45.0, 32.5, 45.0, 45.0, 65.0, 125, true, //
                    33, 122, 100, 45.0, 45.0, 32.5, 45.0, 45.0, 65.0, 125, true, //
                    33, 122, 131, 45.0, 45.0, 32.5, 45.0, 45.0, 65.0, 125, true }, //
            { "AID2", //
                    25, 169, 100, 20.0, 20.0, 10.0, 20.0, 20.0, 20.0, 100, true, //
                    25, 169, 162, 20.0, 20.0, 20.0, 20.0, 20.0, 40.0, 0, true, //
                    null, 0, 100, 0.0, 0.0, 10.0, 0.0, 0.0, 20.0, -100, true, //
                    null, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false }, //
            { "AID3", //
                    -125, 150, 140, 20.0, 20.0, 20.0, 20.0, 20.0, 40.0, 0, true, //
                    null, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, true, //
                    null, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, true, //
                    null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false }, //
            { "AID4", //
                    75, 90, 84, 60.0, 60.0, 30.0, 60.0, 60.0, 60.0, 100, true, //
                    null, 120, 140, 20.0, 20.0, 10.0, 20.0, 20.0, 20.0, 100, true, //
                    null, 120, 140, 20.0, 20.0, 10.0, 20.0, 20.0, 20.0, 100, true, //
                    null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false }, //
            { "AID5", //
                    null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, true, //
                    null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, true, //
                    null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false, //
                    null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false }, //
            { "AID6", //
                    50, 100, 100, 10.0, 10.0, 5.0, 10.0, 10.0, 10.0, 100, true, //
                    50, 100, 100, 10.0, 10.0, 5.0, 10.0, 10.0, 10.0, 100, true, //
                    null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false, //
                    null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false }, //
            { "AID7", //
                    50, null, null, 10.0, 10.0, 5.0, 10.0, 10.0, 10.0, 100, true, //
                    null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false, //
                    null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false, //
                    null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false }, //
            { "AID8", //
                    null, null, 100, 0.0, 0.0, 10.0, 0.0, 0.0, 20.0, -100, true, //
                    null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false, //
                    null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false, //
                    null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false }, //
            { AID_NO_TXN, //
                    null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false, //
                    null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false, //
                    null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false, //
                    null, null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, false }, //

    };

    private void prepareWeekTable() {
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
            case "DepivotedMetrics":
                verifyDepivotedMetrics(records, depivotedMetricsData, false);
                break;
            case "PivotMetrics":
                verifyPivotMetrics(records, pivotMetricsData);
                break;
            case "ReducedDepivotedMetrics":
                verifyDepivotedMetrics(records, depivotedMetricsData, true);
                break;
            case "ExpandedPivotMetrics":
                verifyPivotMetrics(records, pivotMetricsData);
                break;
            case "DepivotedMetricsNoSegment":
                verifyDepivotedMetricsNoSegment(records);
                break;
            default:
                throw new UnsupportedOperationException(String.format("Unknown intermediate source %s", source));
            }
        } catch (Exception ex) {
            throw new RuntimeException("Exception in verifyIntermediateResult", ex);
        }
    }

    private void verifyDepivotedMetrics(Iterator<GenericRecord> records, Object[][] depivotedMetricsData,
            boolean reduced) {
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

            IntStream.range(0, metricsList.size()).forEach(i -> {
                Assert.assertTrue(isObjEquals(record.get(ActivityMetricsUtils.getNameWithPeriod(metricsList.get(i))),
                        expected[i + 2]));
            });
            cnt++;
        }

        int analyticProductNum = 0;
        for (Object[] p : productData) {
            if (ProductType.Analytic.name().equalsIgnoreCase(String.valueOf(p[1]))) {
                analyticProductNum++;
            }
        }
        if (reduced) {
            Assert.assertEquals(cnt, depivotedMetricsData.length - analyticProductNum);
        } else {
            Assert.assertEquals(cnt, depivotedMetricsData.length);
        }

    }

    private void verifyPivotMetrics(Iterator<GenericRecord> records, Object[][] pivotMetricsData) {
        log.info("Verifying pivot metrics table");
        Map<String, Object[]> expectedMetrics = new HashMap<>();
        for (Object[] ent : pivotMetricsData) {
            expectedMetrics.put(ent[0].toString(), ent); // AccountId is unique
        }

        String[] expectedProductIds = new String[] { "PID1", "PID2", "PID3", "PID4" };
        int cnt = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            String key = record.get(InterfaceName.AccountId.name()).toString();
            Object expected = expectedMetrics.get(key);
            Assert.assertNotNull(expected);
            for (int i = 0; i < expectedProductIds.length; i++)
                for (int j = 0; j < metricsList.size(); j++) {
                    ActivityMetrics metrics = metricsList.get(j);
                    String fullMetricsName = ActivityMetricsUtils.getFullName(metrics,
                            expectedProductIds[i]);

                    log.info(String.format("Checking %s: actual = %s, expected = %s", fullMetricsName,
                            String.valueOf(record.get(fullMetricsName)),
                            String.valueOf(expectedMetrics.get(key)[i * metricsList.size() + j + 1])));
            
                    Assert.assertTrue(isObjEquals(record.get(fullMetricsName),
                            expectedMetrics.get(key)[i * metricsList.size() + j + 1]));
                }
            cnt++;
        }
        Assert.assertEquals(cnt, pivotMetricsData.length);
    }

    private void verifyDepivotedMetricsNoSegment(Iterator<GenericRecord> records) {
        log.info("Verifying depivoted metrics without segment in Account");
        ActivityMetrics shareOfWallet = metricsList.stream().filter(m -> m.getMetrics() == InterfaceName.ShareOfWallet)
                .collect(Collectors.toList()).get(0);
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            Assert.assertNull(record.get(ActivityMetricsUtils.getNameWithPeriod(shareOfWallet)));
        }
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
    }

}
