package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;

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

    private GeneralSource source = cleanedWeekTable;

    @Test(groups = "functional", enabled = true)
    public void testTransformation() {
        prepareAccount();
        prepareProduct();
        prepareWeekTable();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmIntermediateSource(cleanedWeekTable, null);
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

        TransformationStepConfig step0 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add(weekTable.getSourceName());
        baseSources.add(account.getSourceName());
        baseSources.add(product.getSourceName());
        step0.setBaseSources(baseSources);
        step0.setTransformer(DataCloudConstants.PURCHASE_METRICS_INITIATOR);
        step0.setTargetSource(cleanedWeekTable.getSourceName());
        step0.setConfiguration("{}");

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step0);

        // -----------
        configuration.setSteps(steps);
        configuration.setVersion(HdfsPathBuilder.dateFormat.format(new Date()));
        configuration.setKeepTemp(true);
        return configuration;
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
                { "AID6", null } //
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
     * 2017-12-28: week = 939, month = 215, quarter = 71, year = 17
     * 
     * last 2 weeks, last month, last quarter, last year
     * 2017-12-14: week = 938, month = 215, quarter = 71, year = 17
     * 2017-12-15: week = 938, month = 215, quarter = 71, year = 17
     * 
     * last 2 months, last quarter, last year
     * 2017-11-30: week = 935, month = 214, quarter = 71, year = 17
     * 2017-11-29: week = 935, month = 214, quarter = 71, year = 17
     * 
     * last 2 quarters, last year
     * 2017-05-31: week = 909, month = 208, quarter = 69, year = 17
     * 2017-05-30: week = 909, month = 208, quarter = 69, year = 17
     * 
     * last 2 years
     * 2016-12-30: week = 887, month = 203, quarter = 69, year = 16
     * 2016-12-29: week = 887, month = 203, quarter = 69, year = 16
     */

    // Test case covered in week: ID all covered, data all available, filter non-analytic product
    private Object[][] weekData = new Object[][] {
            // max txn date
            { "TID001", "AID1", "PID1", 100.0, 100.0, "Week", 940 }, //
            // last week
            { "TID002", "AID1", "PID1", 10.0, 5.0, "Week", 939 }, //
            { "TID003", "AID1", "PID2", 20.0, 10.0, "Week", 939 }, //
            { "TID004", "AID1", "PID3", 30.0, 15.0, "Week", 939 }, //
            { "TID005", "AID1", "PID4", 40.0, 20.0, "Week", 939 }, //
            { "TID006", "AID1", "PID5", 50.0, 25.0, "Week", 939 }, //
            { "TID007", "AID1", "PID6", 60.0, 30.0, "Week", 939 }, //
            // last 2 week
            { "TID008", "AID2", "PID1", 10.0, 5.0, "Week", 938 }, //
            { "TID009", "AID3", "PID2", 20.0, 10.0, "Week", 938 }, //
            { "TID010", "AID4", "PID3", 30.0, 15.0, "Week", 938 }, //
            { "TID011", "AID5", "PID4", 40.0, 20.0, "Week", 938 }, //
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
            default:
                throw new UnsupportedOperationException(String.format("Unknown intermediate source %s", source));
            }
        } catch (Exception ex) {
            throw new RuntimeException("Exception in verifyIntermediateResult", ex);
        }
    }

    private void verifyCleanedWeekTable(Iterator<GenericRecord> records) {
        log.info("Varifying cleaned week table");
        int cnt = 0;
        while (records.hasNext()) {
            log.info(records.next().toString());
            cnt++;
        }
        Assert.assertEquals(cnt, weekData.length - 2);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
    }

}
