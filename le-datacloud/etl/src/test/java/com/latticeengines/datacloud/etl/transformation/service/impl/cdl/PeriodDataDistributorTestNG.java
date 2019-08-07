package com.latticeengines.datacloud.etl.transformation.service.impl.cdl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PERIOD_DATA_DISTRIBUTOR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodDataDistributorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.util.TimeSeriesUtils;

public class PeriodDataDistributorTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(PeriodDataDistributorTestNG.class);

    private static final String DAILY_STORE = "DailyStore";
    private static final String WEEK_STORE = "WeekStore";
    private static final String MONTH_STORE = "MonthStore";

    // Test rebuild mode
    private GeneralSource dailyPeriodIds1 = new GeneralSource("DailyPeriodIds1");
    private GeneralSource dailyAgg1 = new GeneralSource("DailyAgg1");
    private GeneralSource dailyStore = new GeneralSource(DAILY_STORE);
    // Test update mode
    private GeneralSource dailyPeriodIds2 = new GeneralSource("DailyPeriodIds2");
    private GeneralSource dailyAgg2 = new GeneralSource("DailyAgg2");

    // Test rebuild mode
    private GeneralSource multiPeriodIds1 = new GeneralSource("MultiPeriodIds1");
    private GeneralSource multiAgg1 = new GeneralSource("MultiAgg1");
    private GeneralSource weekStore = new GeneralSource(WEEK_STORE);
    private GeneralSource monthStore = new GeneralSource(MONTH_STORE);
    // Test update mode
    private GeneralSource multiPeriodIds2 = new GeneralSource("MultiPeriodIds2");
    private GeneralSource multiAgg2 = new GeneralSource("MultiAgg2");

    private GeneralSource source = monthStore;

    @Test(groups = "functional")
    public void testTransformation() {
        prepareDailyData1();
        prepareDailyData2();
        prepareMultiData1();
        prepareMultiData2();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        confirmIntermediateSource(dailyStore, baseSourceVersion);
        confirmIntermediateSource(weekStore, baseSourceVersion);
        confirmIntermediateSource(monthStore, baseSourceVersion);
        cleanupProgressTables();
    }


    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName(PeriodDataDistributorTestNG.class.getSimpleName());
        configuration.setVersion(targetVersion);

        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add(dailyPeriodIds1.getSourceName());
        baseSources.add(dailyAgg1.getSourceName());
        baseSources.add(dailyStore.getSourceName());
        step1.setBaseSources(baseSources);
        step1.setTransformer(PERIOD_DATA_DISTRIBUTOR);
        step1.setConfiguration(getSinglePeriodConfig());

        TransformationStepConfig step2 = new TransformationStepConfig();
        baseSources = new ArrayList<>();
        baseSources = new ArrayList<>();
        baseSources.add(dailyPeriodIds2.getSourceName());
        baseSources.add(dailyAgg2.getSourceName());
        baseSources.add(dailyStore.getSourceName());
        step2.setBaseSources(baseSources);
        step2.setTransformer(PERIOD_DATA_DISTRIBUTOR);
        step2.setConfiguration(getSinglePeriodConfig());

        TransformationStepConfig step3 = new TransformationStepConfig();
        baseSources = new ArrayList<>();
        baseSources.add(multiPeriodIds1.getSourceName());
        baseSources.add(multiAgg1.getSourceName());
        baseSources.add(weekStore.getSourceName());
        baseSources.add(monthStore.getSourceName());
        step3.setBaseSources(baseSources);
        step3.setTransformer(PERIOD_DATA_DISTRIBUTOR);
        step3.setConfiguration(getMultiPeriodConfig());

        TransformationStepConfig step4 = new TransformationStepConfig();
        baseSources = new ArrayList<>();
        baseSources.add(multiPeriodIds2.getSourceName());
        baseSources.add(multiAgg2.getSourceName());
        baseSources.add(weekStore.getSourceName());
        baseSources.add(monthStore.getSourceName());
        step4.setBaseSources(baseSources);
        step4.setTransformer(PERIOD_DATA_DISTRIBUTOR);
        step4.setConfiguration(getMultiPeriodConfig());

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step1);
        steps.add(step2);
        steps.add(step3);
        steps.add(step4);

        // -----------
        configuration.setSteps(steps);
        return configuration;
    }

    private String getSinglePeriodConfig() {
        PeriodDataDistributorConfig config = new PeriodDataDistributorConfig();
        config.setCleanupFirst(true);
        return JsonUtils.serialize(config);
    }

    private String getMultiPeriodConfig() {
        PeriodDataDistributorConfig config = new PeriodDataDistributorConfig();
        config.setMultiPeriod(true);
        Map<String, Integer> periodNameIdxes = ImmutableMap.of( //
                PeriodStrategy.Template.Week.name(), 2, //
                PeriodStrategy.Template.Month.name(), 3);
        config.setTransactionIdxes(periodNameIdxes);
        config.setCleanupFirst(true);
        return JsonUtils.serialize(config);
    }

    private String[] dailyIdFields = { InterfaceName.PeriodId.name() };

    private Object[][] dailyIds1 = new Object[][] { //
            // test case: single txn
            { 1 }, //
            // test case: multiple txn
            { 2 }, //
            // test case: no txn
            { 3 } //
    };

    private String[] dailyDataFields = { InterfaceName.Id.name(), InterfaceName.PeriodId.name() };

    // Test data is designed to use Id as identifier
    private Object[][] dailyData1 = new Object[][] { //
            // test case: single txn
            { "T1", 1 }, //
            // test case: multiple txn
            { "T2", 2 }, //
            { "T3", 2 }, //
    };

    private void prepareDailyData1() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        for (String periodIdFld : dailyIdFields) {
            if (InterfaceName.PeriodId.name().equals(periodIdFld)) {
                schema.add(Pair.of(periodIdFld, Integer.class));
            } else {
                schema.add(Pair.of(periodIdFld, String.class));
            }
        }
        uploadBaseSourceData(dailyPeriodIds1.getSourceName(), baseSourceVersion, schema, dailyIds1);

        schema = new ArrayList<>();
        for (String txnFld : dailyDataFields) {
            if (InterfaceName.PeriodId.name().equals(txnFld)) {
                schema.add(Pair.of(txnFld, Integer.class));
            } else {
                schema.add(Pair.of(txnFld, String.class));
            }
        }
        uploadBaseSourceData(dailyAgg1.getSourceName(), baseSourceVersion, schema, dailyData1);

        String dailyStorePath = hdfsPathBuilder.constructTransformationSourceDir(dailyStore, baseSourceVersion)
                .toString();
        try {
            HdfsUtils.mkdir(yarnConfiguration, dailyStorePath);
        } catch (IOException e) {
            throw new RuntimeException("Fail to create daily store: " + dailyStorePath, e);
        }
        hdfsSourceEntityMgr.setCurrentVersion(dailyStore, baseSourceVersion);
    }

    private Object[][] dailyIds2 = new Object[][] { //
            // test case: update existing period
            { 1 }, //
            // test case: add new period
            { 3 } //
    };

    // Test data is designed to use Id as identifier
    private Object[][] dailyData2 = new Object[][] { //
            // test case: update existing period
            { "T4", 1 }, //
            // test case: add new period
            { "T5", 3 }, //
            { "T6", 3 }, //
    };

    private void prepareDailyData2() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        for (String periodIdFld : dailyIdFields) {
            if (InterfaceName.PeriodId.name().equals(periodIdFld)) {
                schema.add(Pair.of(periodIdFld, Integer.class));
            } else {
                schema.add(Pair.of(periodIdFld, String.class));
            }
        }
        uploadBaseSourceData(dailyPeriodIds2.getSourceName(), baseSourceVersion, schema, dailyIds2);

        schema = new ArrayList<>();
        for (String txnFld : dailyDataFields) {
            if (InterfaceName.PeriodId.name().equals(txnFld)) {
                schema.add(Pair.of(txnFld, Integer.class));
            } else {
                schema.add(Pair.of(txnFld, String.class));
            }
        }
        uploadBaseSourceData(dailyAgg2.getSourceName(), baseSourceVersion, schema, dailyData2);
    }

    private String[] multiIdFields = { InterfaceName.PeriodId.name(), InterfaceName.PeriodName.name() };

    private Object[][] multiIds1 = new Object[][] { //
            // test case: single txn
            { 1, PeriodStrategy.Template.Week.name() }, //
            { 1, PeriodStrategy.Template.Month.name() }, //
            // test case: multiple txn
            { 2, PeriodStrategy.Template.Week.name() }, //
            { 2, PeriodStrategy.Template.Month.name() }, //
            // test case: no txn
            { 3, PeriodStrategy.Template.Week.name() }, //
            { 3, PeriodStrategy.Template.Month.name() }, //
    };

    private String[] multiDataFields = { InterfaceName.Id.name(), InterfaceName.PeriodId.name(),
            InterfaceName.PeriodName.name() };

    // Test data is designed to use Id as identifier
    private Object[][] multiData1 = new Object[][] { //
            // test case: single txn
            { "T1", 1, PeriodStrategy.Template.Week.name() }, //
            { "T2", 1, PeriodStrategy.Template.Month.name() }, //
            // test case: multiple txn
            { "T3", 2, PeriodStrategy.Template.Week.name() }, //
            { "T4", 2, PeriodStrategy.Template.Week.name() }, //
            { "T5", 2, PeriodStrategy.Template.Month.name() }, //
            { "T6", 2, PeriodStrategy.Template.Month.name() }, //
    };

    private void prepareMultiData1() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        for (String periodIdFld : multiIdFields) {
            if (InterfaceName.PeriodId.name().equals(periodIdFld)) {
                schema.add(Pair.of(periodIdFld, Integer.class));
            } else {
                schema.add(Pair.of(periodIdFld, String.class));
            }
        }
        uploadBaseSourceData(multiPeriodIds1.getSourceName(), baseSourceVersion, schema, multiIds1);

        schema = new ArrayList<>();
        for (String txnFld : multiDataFields) {
            if (InterfaceName.PeriodId.name().equals(txnFld)) {
                schema.add(Pair.of(txnFld, Integer.class));
            } else {
                schema.add(Pair.of(txnFld, String.class));
            }
        }
        uploadBaseSourceData(multiAgg1.getSourceName(), baseSourceVersion, schema, multiData1);

        String weekStorePath = hdfsPathBuilder.constructTransformationSourceDir(weekStore, baseSourceVersion)
                .toString();
        try {
            HdfsUtils.mkdir(yarnConfiguration, weekStorePath);
        } catch (IOException e) {
            throw new RuntimeException("Fail to create week store: " + weekStorePath, e);
        }
        hdfsSourceEntityMgr.setCurrentVersion(weekStore, baseSourceVersion);

        String monthStorePath = hdfsPathBuilder.constructTransformationSourceDir(monthStore, baseSourceVersion)
                .toString();
        try {
            HdfsUtils.mkdir(yarnConfiguration, monthStorePath);
        } catch (IOException e) {
            throw new RuntimeException("Fail to create month store: " + monthStorePath, e);
        }
        hdfsSourceEntityMgr.setCurrentVersion(monthStore, baseSourceVersion);
    }

    private Object[][] multiIds2 = new Object[][] { //
            // test case: update existing period
            { 1, PeriodStrategy.Template.Week.name() }, //
            { 1, PeriodStrategy.Template.Month.name() }, //
            // test case: add new period
            { 3, PeriodStrategy.Template.Week.name() }, //
            { 3, PeriodStrategy.Template.Month.name() } //
    };

    // Test data is designed to use Id as identifier
    private Object[][] multiData2 = new Object[][] { //
            // test case: update existing period
            { "T7", 1, PeriodStrategy.Template.Week.name() }, //
            { "T8", 1, PeriodStrategy.Template.Month.name() }, //
            // test case: add new period
            { "T9", 3, PeriodStrategy.Template.Week.name() }, //
            { "T10", 3, PeriodStrategy.Template.Week.name() }, //
            { "T11", 3, PeriodStrategy.Template.Month.name() }, //
            { "T12", 3, PeriodStrategy.Template.Month.name() }, //
    };

    private void prepareMultiData2() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        for (String periodIdFld : multiIdFields) {
            if (InterfaceName.PeriodId.name().equals(periodIdFld)) {
                schema.add(Pair.of(periodIdFld, Integer.class));
            } else {
                schema.add(Pair.of(periodIdFld, String.class));
            }
        }
        uploadBaseSourceData(multiPeriodIds2.getSourceName(), baseSourceVersion, schema, multiIds2);

        schema = new ArrayList<>();
        for (String txnFld : multiDataFields) {
            if (InterfaceName.PeriodId.name().equals(txnFld)) {
                schema.add(Pair.of(txnFld, Integer.class));
            } else {
                schema.add(Pair.of(txnFld, String.class));
            }
        }
        uploadBaseSourceData(multiAgg2.getSourceName(), baseSourceVersion, schema, multiData2);
    }

    @Override
    protected void verifyIntermediateResult(String source, String version, Iterator<GenericRecord> records) {
        log.info("Verifying intermediate source " + source);
        switch (source) {
        case DAILY_STORE:
            verifyDataCorrectness(mergeInputs(dailyData1, dailyData2, dailyDataFields), records, dailyDataFields);
            Set<Integer> expectedPeriods = getPeriods(dailyIdFields, dailyIds1, dailyIds2);
            verifyPeriodPartition(dailyStore, expectedPeriods);
            break;
        case WEEK_STORE:
            verifyDataCorrectness(
                    mergeInputs(filterInputByPeriod(multiData1, PeriodStrategy.Template.Week.name(), multiDataFields),
                            filterInputByPeriod(multiData2, PeriodStrategy.Template.Week.name(), multiDataFields),
                            multiDataFields),
                    records, multiDataFields);
            expectedPeriods = getPeriods(multiIdFields,
                    filterInputByPeriod(multiIds1, PeriodStrategy.Template.Week.name(), multiIdFields),
                    filterInputByPeriod(multiIds2, PeriodStrategy.Template.Week.name(), multiIdFields));
            verifyPeriodPartition(weekStore, expectedPeriods);
            break;
        case MONTH_STORE:
            verifyDataCorrectness(
                    mergeInputs(filterInputByPeriod(multiData1, PeriodStrategy.Template.Month.name(), multiDataFields),
                            filterInputByPeriod(multiData2, PeriodStrategy.Template.Month.name(), multiDataFields),
                            multiDataFields),
                    records, multiDataFields);
            expectedPeriods = getPeriods(multiIdFields,
                    filterInputByPeriod(multiIds1, PeriodStrategy.Template.Month.name(), multiIdFields),
                    filterInputByPeriod(multiIds2, PeriodStrategy.Template.Month.name(), multiIdFields));
            verifyPeriodPartition(monthStore, expectedPeriods);
            break;
        default:
            throw new IllegalArgumentException("Unknown intermediate source " + source);
        }
    }

    /**
     * Verify every record and every attribute has expected value
     *
     * @param inputData
     * @param records
     * @param fields
     */
    private void verifyDataCorrectness(Object[][] inputData, Iterator<GenericRecord> records, String[] fields) {
        Map<String, List<Object>> expectedMap = Arrays.stream(inputData)
                .collect(Collectors.toMap(arr -> (String) arr[0], arr -> Arrays.asList(arr)));
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            Assert.assertNotNull(record);
            // Test data is designed to use Id as identifier
            Assert.assertNotNull(record.get(InterfaceName.Id.name()));
            String id = record.get(InterfaceName.Id.name()).toString();
            List<Object> expected = expectedMap.get(id);
            Assert.assertNotNull(expected);
            List<Object> actual = Arrays.stream(fields)
                    .map(field -> record.get(field) == null ? null
                            : (record.get(field) instanceof Utf8 ? record.get(field).toString() : record.get(field)))
                    .collect(Collectors.toList());
            Assert.assertEquals(actual, expected);
            expectedMap.remove(id);
        }
        Assert.assertTrue(expectedMap.isEmpty());
    }

    /**
     * Verify PeriodId showing up in all distributed file names cover all the
     * expected PeriodId
     *
     * @param periodStore
     * @param expectedPeriods
     */
    private void verifyPeriodPartition(Source periodStore, Set<Integer> expectedPeriods) {
        String sourcePath = hdfsPathBuilder.constructTransformationSourceDir(periodStore, baseSourceVersion).toString();
        List<String> files;
        try {
            files = HdfsUtils.getFilesForDir(yarnConfiguration, sourcePath);
        } catch (IOException e) {
            throw new RuntimeException("Fail to list files under HDFS path " + sourcePath);
        }
        Set<Integer> actualPeriod = files.stream()
                .map(file -> TimeSeriesUtils.getPeriodFromFileName(new Path(file).getName()))
                .collect(Collectors.toSet());
        Assert.assertEquals(actualPeriod, expectedPeriods);
    }

    /**
     * Merge input1 and input2 -- input2 is to update period store setup by
     * input1. If there is overlapped PeriodId between input1 and input2,
     * input1's data with overlapped PeriodId is wiped out
     *
     * @param input1
     * @param input2
     * @param fields
     * @return
     */
    private Object[][] mergeInputs(Object[][] input1, Object[][] input2, String[] fields) {
        int periodFldIdx = IntStream.range(0, fields.length) //
                .filter(i -> InterfaceName.PeriodId.name().equals(fields[i])) //
                .findFirst() //
                .orElse(-1);
        if (periodFldIdx == -1) {
            throw new RuntimeException("Cannot find PeriodId field in fields");
        }

        List<Object[]> merged = Arrays.stream(input2) //
                .collect(Collectors.toList());
        Set<Integer> periodId2 = Arrays.stream(input2) //
                .map(data -> (Integer) data[periodFldIdx]) //
                .collect(Collectors.toSet());
        merged.addAll(
                Arrays.stream(input1).filter(data -> !periodId2.contains(data[periodFldIdx])) //
                        .collect(Collectors.toList()));
        return merged.toArray(new Object[merged.size()][]);

    }

    /**
     * Filter input data by specified PeriodName
     *
     * @param input
     * @param periodName
     * @param fields
     * @return
     */
    private Object[][] filterInputByPeriod(Object[][] input, String periodName, String[] fields) {
        int periodNameFldIdx = IntStream.range(0, fields.length) //
                .filter(i -> InterfaceName.PeriodName.name().equals(fields[i])) //
                .findFirst() //
                .orElse(-1);
        if (periodNameFldIdx == -1) {
            throw new RuntimeException("Cannot find PeriodName field in fields");
        }
        return Arrays.stream(input) //
                .filter(row -> periodName.equals(row[periodNameFldIdx]))
                .toArray(size -> new Object[size][input[0].length]);
    }

    /**
     * Get all distinct PeriodIds from all the inputs
     *
     * @param idFields
     * @param periodInputs
     * @return
     */
    private Set<Integer> getPeriods(String[] idFields, Object[][]... periodInputs) {
        int periodIdFldIdx = IntStream.range(0, idFields.length) //
                .filter(i -> InterfaceName.PeriodId.name().equals(idFields[i])) //
                .findFirst() //
                .orElse(-1);
        if (periodIdFldIdx == -1) {
            throw new RuntimeException("Cannot find PeriodId field in idFields");
        }
        Set<Integer> periods = new HashSet<>();
        for (Object[][] periodInput : periodInputs) {
            for (Object[] row : periodInput) {
                periods.add((Integer) row[periodIdFldIdx]);
            }
        }
        return periods;
    }

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
    }

}
