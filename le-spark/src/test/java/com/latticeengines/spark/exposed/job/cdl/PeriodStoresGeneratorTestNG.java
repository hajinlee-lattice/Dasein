package com.latticeengines.spark.exposed.job.cdl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.activity.ActivityRowReducer;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ActivityStoreSparkIOMetadata;
import com.latticeengines.domain.exposed.spark.cdl.ActivityStoreSparkIOMetadata.Details;
import com.latticeengines.domain.exposed.spark.cdl.DailyStoreToPeriodStoresJobConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;
import com.latticeengines.spark.util.DeriveAttrsUtils;

public class PeriodStoresGeneratorTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PeriodStoresGeneratorTestNG.class);

    private static final String PathPatternId = InterfaceName.PathPatternId.name();
    private static final String SourceMediumId = InterfaceName.SourceMediumId.name();
    private static final String AccountId = InterfaceName.AccountId.name();
    private static final String PeriodId = InterfaceName.PeriodId.name();
    private static final String OpportunityId = InterfaceName.OpportunityId.name();
    private static final String StageName = InterfaceName.StageName.name();
    private static final String StageNameId = InterfaceName.StageNameId.name();
    private static final String LastModifiedDate = InterfaceName.LastModifiedDate.name();
    private static final String Count = InterfaceName.__Row_Count__.name();
    private static final String StreamDate = InterfaceName.__StreamDate.name();
    private static final String StreamDateId = InterfaceName.__StreamDateId.name();
    private static final String LastActivityDate = InterfaceName.LastActivityDate.name();
    private static final String DATE_ATTR = InterfaceName.LastModifiedDate.name();
    private static final String PeriodIdForPartition = DeriveAttrsUtils.PARTITION_COL_PREFIX() + PeriodId;
    private static final String VERSION_COL = DeriveAttrsUtils.VERSION_COL();
    // DateId in daily store table is not used while generating period stores

    private static List<String> OUTPUT_FIELDS_NO_REDUCER;
    private static List<String> OUTPUT_FIELDS_WITH_REDUCER;
    private static List<String> PERIODS = Arrays.asList(PeriodStrategy.Template.Week.name(), PeriodStrategy.Template.Month.name());
    private static List<String> SINGLE_PERIOD = Collections.singletonList(PeriodStrategy.Template.Week.name());
    private static AtlasStream INCREMENTAL_STREAM;
    private static final String STREAM_ID = "abc123";

    private static final String STAGE_WON = "won";
    private static final String STAGE_WON_ID = "1";
    private static final String STAGE_CLOSE = "close";
    private static final String STAGE_CLOSE_ID = "2";
    private static final String STAGE_NEW = "newStage";
    private static final String STAGE_NEW_ID = "3";
    private static final String STAGE_OLD = "oldStage";
    private static final String STAGE_OLD_ID = "4";

    private static final String MODEL_1_ID = "1";
    private static final String MODEL_2_ID = "2";

    private static final String EVAL_DATE = "2019-10-28";

    private static final long OLD_VERSION = 10L;
    private static final long NEW_VERSION = 99L;

    // TODO - followings should move to use interface name
    private static final String modelName = "modelName";
    private static final String modelNameId = "modelNameId";
    private static final String hasIntent = "hasIntent";

    @Test(groups = "functional")
    public void test() {
        List<String> inputs = Collections.singletonList(setupNoReducer());
        DailyStoreToPeriodStoresJobConfig config = new DailyStoreToPeriodStoresJobConfig();
        config.evaluationDate = EVAL_DATE;
        config.streams = Collections.singletonList(setupStream());
        config.inputMetadata = createInputMetadata();
        SparkJobResult result = runSparkJob(PeriodStoresGenerator.class, config, inputs, getWorkspace());
        Assert.assertNotNull(result.getOutput());
        ActivityStoreSparkIOMetadata metadata = JsonUtils.deserialize(result.getOutput(), ActivityStoreSparkIOMetadata.class);
        Assert.assertNotNull(metadata);
        Assert.assertNotNull(metadata.getMetadata().get(STREAM_ID));
        Assert.assertEquals(metadata.getMetadata().get(STREAM_ID).getLabels(), PERIODS);
        verify(result, Arrays.asList(this::verifyWeekPeriodStore, this::verifyMonthPeriodStore));
    }

    @Test(groups = "functional")
    public void testWithReducer() {
        List<String> inputs = Collections.singletonList(setupImportDataForReducerStream());
        DailyStoreToPeriodStoresJobConfig config = new DailyStoreToPeriodStoresJobConfig();
        config.evaluationDate = EVAL_DATE;
        config.streams = Collections.singletonList(setupStreamWithReducer());
        config.inputMetadata = createInputMetadata();
        SparkJobResult result = runSparkJob(PeriodStoresGenerator.class, config, inputs, getWorkspace());
        ActivityStoreSparkIOMetadata metadata = JsonUtils.deserialize(result.getOutput(), ActivityStoreSparkIOMetadata.class);
        Assert.assertNotNull(metadata);
        verify(result, Collections.singletonList(this::verifyReduced));
    }

    @Test(groups = "functional")
    public void testIncr() {
        List<String> inputs = setupIncrStream(true);
        DailyStoreToPeriodStoresJobConfig config = new DailyStoreToPeriodStoresJobConfig();
        config.evaluationDate = EVAL_DATE;
        config.streams = Collections.singletonList(INCREMENTAL_STREAM);
        config.incrementalStreams.add(STREAM_ID);
        ActivityStoreSparkIOMetadata inputMetadata = new ActivityStoreSparkIOMetadata();
        Map<String, Details> detailsMap = new HashMap<>();
        Details details = new Details();
        details.setStartIdx(0);
        details.setLabels(SINGLE_PERIOD);
        detailsMap.put(STREAM_ID, details);
        inputMetadata.setMetadata(detailsMap);
        config.inputMetadata = inputMetadata;
        SparkJobResult result = runSparkJob(PeriodStoresGenerator.class, config, inputs, getWorkspace());
        log.info("Output metadata: {}", result.getOutput());
    }

    @Test(groups = "functional")
    public void testIncrNoBatch() {
        List<String> inputs = setupIncrStream(false);
        DailyStoreToPeriodStoresJobConfig config = new DailyStoreToPeriodStoresJobConfig();
        config.evaluationDate = EVAL_DATE;
        config.streams = Collections.singletonList(INCREMENTAL_STREAM);
        config.incrementalStreams.add(STREAM_ID);
        config.streamsWithNoBatch.add(STREAM_ID);
        ActivityStoreSparkIOMetadata inputMetadata = new ActivityStoreSparkIOMetadata();
        Map<String, Details> detailsMap = new HashMap<>();
        Details details = new Details();
        details.setStartIdx(0);
        details.setLabels(Collections.emptyList());
        detailsMap.put(STREAM_ID, details);
        inputMetadata.setMetadata(detailsMap);
        config.inputMetadata = inputMetadata;
        SparkJobResult result = runSparkJob(PeriodStoresGenerator.class, config, inputs, getWorkspace());
        log.info("Output metadata: {}", result.getOutput());
    }

    @Test(groups = "functional")
    public void testIncrReducer() {
        List<String> inputs = Arrays.asList(setupIncrReducerBatchStore(), setupIncrReducerDeltaImport());
        DailyStoreToPeriodStoresJobConfig config = new DailyStoreToPeriodStoresJobConfig();
        config.evaluationDate = EVAL_DATE;
        config.streams = Collections.singletonList(setupStreamWithReducer());
        config.incrementalStreams.add(STREAM_ID);
        ActivityStoreSparkIOMetadata inputMetadata = new ActivityStoreSparkIOMetadata();
        Map<String, Details> detailsMap = new HashMap<>();
        Details details = new Details();
        details.setStartIdx(0);
        details.setLabels(SINGLE_PERIOD);
        detailsMap.put(STREAM_ID, details);
        inputMetadata.setMetadata(detailsMap);
        config.inputMetadata = inputMetadata;
        SparkJobResult result = runSparkJob(PeriodStoresGenerator.class, config, inputs, getWorkspace());
    }

    @Test(groups = "functional")
    public void testIntentActivity() {
        List<String> inputs = Collections.singletonList(setupIntentActivityData());
        DailyStoreToPeriodStoresJobConfig config = new DailyStoreToPeriodStoresJobConfig();
        config.evaluationDate = EVAL_DATE;
        config.streams = Collections.singletonList(setupIntentActivityStream());
        ActivityStoreSparkIOMetadata inputMetadata = new ActivityStoreSparkIOMetadata();
        Map<String, Details> detailsMap = new HashMap<>();
        Details details = new Details();
        details.setStartIdx(0);
        details.setLabels(SINGLE_PERIOD);
        detailsMap.put(STREAM_ID, details);
        inputMetadata.setMetadata(detailsMap);
        config.inputMetadata = inputMetadata;
        SparkJobResult result = runSparkJob(PeriodStoresGenerator.class, config, inputs, getWorkspace());
    }

    @Test(groups = "functional")
    public void testIntentIncr() {
        List<String> inputs = Arrays.asList(setupIntentBatchStore(), setupIntentDeltaImport());
        DailyStoreToPeriodStoresJobConfig config = new DailyStoreToPeriodStoresJobConfig();
        config.evaluationDate = EVAL_DATE;
        config.streams = Collections.singletonList(setupIntentActivityStream());
        config.incrementalStreams.add(STREAM_ID);
        ActivityStoreSparkIOMetadata inputMetadata = new ActivityStoreSparkIOMetadata();
        Map<String, Details> detailsMap = new HashMap<>();
        Details details = new Details();
        details.setStartIdx(0);
        details.setLabels(SINGLE_PERIOD);
        detailsMap.put(STREAM_ID, details);
        inputMetadata.setMetadata(detailsMap);
        config.inputMetadata = inputMetadata;
        SparkJobResult result = runSparkJob(PeriodStoresGenerator.class, config, inputs, getWorkspace());
    }

    private String setupIncrReducerBatchStore() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(AccountId, String.class), //
                Pair.of(OpportunityId, String.class), //
                Pair.of(StageName, String.class), //
                Pair.of(StageNameId, String.class),
                Pair.of(LastModifiedDate, Long.class), // epoch
                Pair.of(StreamDate, String.class),
                Pair.of(StreamDateId, Integer.class),
                Pair.of(LastActivityDate, Long.class),
                Pair.of(Count, Integer.class),
                Pair.of(PeriodId, Integer.class),
                Pair.of(PeriodIdForPartition, Integer.class)
        );
        Object[][] data = new Object[][]{
                {"acc1", "opp1", STAGE_OLD, STAGE_OLD_ID, 1L, "2019-10-23", 49847, 1L, 1, 1034, 1034}, // will be replaced by delta
                {"acc1", "opp2", STAGE_CLOSE, STAGE_CLOSE_ID, 999L, "2019-10-25", 49849, 999L, 1, 1034, 1034}, // no change since affected but newer than delta
                {"acc1", "opp3", STAGE_WON, STAGE_WON_ID, 1L, "2019-10-23", 49847, 1L, 1, 1034, 1034}, // no change since delta not affecting opportunity
                {"acc1", "opp1", STAGE_CLOSE, STAGE_CLOSE_ID, 999L, "2020-10-23", 50263, 999L, 1, 1086, 1086}, // no change since not affected
                {"acc1", "opp1", STAGE_OLD, STAGE_OLD_ID, 0L, "2000-01-01", 41633, 0L, 1, 0, 0} // over retention days, will be removed
        };
        return uploadHdfsDataUnit(data, fields);
    }

    private String setupIncrReducerDeltaImport() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(AccountId, String.class), //
                Pair.of(OpportunityId, String.class),
                Pair.of(StageName, String.class),
                Pair.of(StageNameId, String.class),
                Pair.of(LastModifiedDate, Long.class),
                Pair.of(StreamDate, String.class),
                Pair.of(StreamDateId, Integer.class),
                Pair.of(Count, Integer.class),
                Pair.of(LastActivityDate, Long.class)
        );
        Object[][] data = new Object[][]{
                {"acc1", "opp1", STAGE_NEW, STAGE_NEW_ID, 2L, "2019-10-24", 49848, 1, 2L},
                {"acc1", "opp2", STAGE_OLD, STAGE_OLD_ID, 0L, "2019-10-22", 49846, 1, 0L}
        };
        return uploadHdfsDataUnit(data, fields);
    }

    private String setupIntentBatchStore() {
        List<Pair<String, Class<?>>> fields = Arrays.asList(
                Pair.of(AccountId, String.class),
                Pair.of(modelNameId, String.class),
                Pair.of(PeriodId, Integer.class),
                Pair.of(hasIntent, Boolean.class),
                Pair.of(Count, Integer.class),
                Pair.of(LastActivityDate, Long.class),
                Pair.of(VERSION_COL, Long.class),
                Pair.of(PeriodIdForPartition, Integer.class)
        );
        Object[][] data = new Object[][]{
                {"acc1", MODEL_1_ID, 1031, true, 10, 0L, OLD_VERSION, 1031},
                {"acc2", MODEL_1_ID, 1031, true, 10, 0L, OLD_VERSION, 1031},
                {"acc1", MODEL_1_ID, 1032, true, 10, 0L, OLD_VERSION, 1032}
        };
        return uploadHdfsDataUnit(data, fields);
    }

    private String setupIntentDeltaImport() {
        List<Pair<String, Class<?>>> fields = Arrays.asList(
                Pair.of(AccountId, String.class),
                Pair.of(modelNameId, String.class),
                Pair.of(StreamDate, String.class),
                Pair.of(StreamDateId, Integer.class),
                Pair.of(Count, Integer.class),
                Pair.of(LastActivityDate, Long.class),
                Pair.of(VERSION_COL, Long.class)
        );
        Object[][] data = new Object[][]{
                {"acc3", MODEL_1_ID, "2019-10-01", 49825, 2, 0L, NEW_VERSION},
                {"acc1", MODEL_1_ID, "2019-10-02", 49826, 2, 0L, NEW_VERSION},
                {"acc1", MODEL_1_ID, "2019-10-01", 49825, 2, 0L, OLD_VERSION} // attached to dailyStoreDelta while doing dailyStore incremental update
        };
        return uploadHdfsDataUnit(data, fields);
    }

    private ActivityStoreSparkIOMetadata createInputMetadata() {
        ActivityStoreSparkIOMetadata inputMetadata = new ActivityStoreSparkIOMetadata();
        Map<String, Details> metadata = new HashMap<>();
        Details details = new Details();
        details.setStartIdx(0);
        metadata.put(STREAM_ID, details);
        inputMetadata.setMetadata(metadata);
        return inputMetadata;
    }

    private String setupNoReducer() {
        List<Pair<String, Class<?>>> inputFields = Arrays.asList( //
                Pair.of(AccountId, String.class), //
                Pair.of(PathPatternId, String.class), //
                Pair.of(SourceMediumId, String.class), // this dimension is not selected for stream
                Pair.of(StreamDate, String.class), //
                Pair.of(Count, Integer.class) //
        );
        Object[][] data = new Object[][]{ //
                {"1", "pp1", "s1", "2019-10-01", 1}, //
                {"1", "pp1", "s1", "2019-10-02", 4}, //
                {"1", "pp1", "s2", "2019-10-03", 3}, //
                {"2", "pp1", "s2", "2019-10-01", 5}, //
                {"2", "pp1", "s3", "2019-10-08", 7}, //
                {"2", "pp2", "s3", "2019-10-08", 6}, //
        };
        return uploadHdfsDataUnit(data, inputFields);
    }

    private String setupImportDataForReducerStream() {
        List<Pair<String, Class<?>>> inputFields = Arrays.asList( //
                Pair.of(AccountId, String.class), //
                Pair.of(OpportunityId, String.class), //
                Pair.of(StageName, String.class), //
                Pair.of(DATE_ATTR, String.class), //
                Pair.of(StreamDate, String.class), //
                Pair.of(Count, Integer.class), //
                Pair.of(LastActivityDate, Long.class)
        );
        Object[][] data = new Object[][]{
                {"acc1", "opp1", "open", "Oct 21, 2018 18:37", "2018-10-21", 1, 1L},
                {"acc1", "opp1", "dev", "Oct 22, 2018 19:37", "2018-10-22", 1, 2L},
                {"acc1", "opp1", "won", "Oct 23, 2018 20:37", "2018-10-23", 1, 3L},
                {"acc2", "opp1", "close", "Oct 24, 2018 20:37", "2018-10-24", 1, 4L},
                {"acc2", "opp2", "open", "Oct 29, 2018 20:37", "2018-10-29", 1, 9L}
        };
        return uploadHdfsDataUnit(data, inputFields);
    }

    private List<String> setupIncrStream(boolean withBatch) {
        if (INCREMENTAL_STREAM == null) {
            setupIncrStream();
        }
        List<String> inputs = new ArrayList<>();
        if (withBatch) {
            inputs.add(setupPeriodBatchStore());
        }
        inputs.add(setupDeltaImport());
        return inputs;
    }

    private String setupDeltaImport() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(AccountId, String.class), //
                Pair.of(PathPatternId, String.class), //
                Pair.of(SourceMediumId, String.class), //
                Pair.of(StreamDate, String.class), //
                Pair.of(Count, Integer.class), //
                Pair.of(LastActivityDate, Integer.class)
        );
        Object[][] data = new Object[][]{ //
                {"a1", "pp1", "s1", "2019-10-01", 1, 49825}, //
                {"a1", "pp1", "s1", "2019-10-02", 4, 49826}, //
                {"a1", "pp1", "s1", "2019-11-01", 3, 49857}, //
                {"a10", "pp1", "s1", "2019-10-02", 2, 49826}, //
                {"a10", "pp1", "s1", "2019-11-02", 9, 49858}
        };
        return uploadHdfsDataUnit(data, fields);
    }

    private String setupPeriodBatchStore() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(AccountId, String.class), //
                Pair.of(PathPatternId, String.class), //
                Pair.of(SourceMediumId, String.class),
                Pair.of(Count, Integer.class), //
                Pair.of(LastActivityDate, Integer.class), //
                Pair.of(PeriodId, Integer.class), //
                Pair.of(PeriodIdForPartition, Integer.class)
        );
        Object[][] data = new Object[][]{ //
                {"a1", "pp1", "s1", 1, 49825, 1031, 1031}, // 2019-10-01 -> 1031
        };
        return uploadHdfsDataUnit(data, fields);
    }

    private String setupIntentActivityData() {
        List<Pair<String, Class<?>>> fields = Arrays.asList(
                Pair.of(AccountId, String.class),
                Pair.of(modelNameId, String.class),
                Pair.of(StreamDate, String.class),
                Pair.of(StreamDateId, Integer.class),
                Pair.of(Count, Integer.class),
                Pair.of(LastActivityDate, Long.class),
                Pair.of(VERSION_COL, Long.class),
                Pair.of(hasIntent, Boolean.class)
        );
        Object[][] data = new Object[][]{
                {"a1", MODEL_1_ID, "2019-10-01", 49825, 1, 0L, 10L, true},
                {"a1", MODEL_1_ID, "2019-10-02", 49826, 1, 0L, 10L, true},
                {"a1", MODEL_2_ID, "2019-10-01", 49825, 1, 0L, 10L, true},
                {"a2", MODEL_1_ID, "2019-10-01", 49825, 1, 0L, 10L, true},
                {"a2", MODEL_1_ID, "2019-10-08", 49832, 1, 0L, 10L, true}
        };
        return uploadHdfsDataUnit(data, fields);
    }

    private AtlasStream setupStream() {
        AtlasStream stream = new AtlasStream();
        stream.setStreamId(STREAM_ID);
        stream.setPeriods(PERIODS);
        stream.setDimensions(Collections.singletonList(prepareDimension(PathPatternId)));
        stream.setAggrEntities(Collections.singletonList(BusinessEntity.Account.name()));

        return stream;
    }

    private AtlasStream setupStreamWithReducer() {
        AtlasStream stream = new AtlasStream();
        stream.setStreamId(STREAM_ID);
        stream.setPeriods(SINGLE_PERIOD);
        stream.setDimensions(Arrays.asList(prepareDimension(OpportunityId), prepareDimension(StageName)));
        stream.setAggrEntities(Collections.singletonList(BusinessEntity.Account.name()));
        stream.setReducer(prepareReducer());
        stream.setRetentionDays(30);

        return stream;
    }

    private void setupIncrStream() {
        AtlasStream stream = new AtlasStream();
        stream.setStreamId(STREAM_ID);
        stream.setPeriods(SINGLE_PERIOD);
        stream.setDimensions(Arrays.asList(prepareDimension(PathPatternId), prepareDimension(SourceMediumId)));
        stream.setAggrEntities(Collections.singletonList(BusinessEntity.Account.name()));

        INCREMENTAL_STREAM = stream;
    }

    private AtlasStream setupIntentActivityStream() {
        AtlasStream stream = new AtlasStream();
        stream.setStreamId(STREAM_ID);
        stream.setPeriods(SINGLE_PERIOD);
        stream.setDimensions(Collections.singletonList(prepareDimension(modelNameId)));
        stream.setAggrEntities(Collections.singletonList(BusinessEntity.Account.name()));
        StreamAttributeDeriver deriver = new StreamAttributeDeriver();
        deriver.setCalculation(StreamAttributeDeriver.Calculation.TRUE);
        deriver.setTargetAttribute(hasIntent);
        deriver.setTargetFundamentalType(FundamentalType.BOOLEAN);
        stream.setAttributeDerivers(Collections.singletonList(deriver));
        return stream;
    }

    private ActivityRowReducer prepareReducer() {
        ActivityRowReducer reducer = new ActivityRowReducer();
        reducer.setGroupByFields(Collections.singletonList(OpportunityId));
        reducer.setArguments(Collections.singletonList(DATE_ATTR));
        reducer.setOperator(ActivityRowReducer.Operator.Latest);
        return reducer;
    }

    private StreamDimension prepareDimension(String name) {
        StreamDimension dim = new StreamDimension();
        dim.setName(name);
        return dim;
    }

    private Boolean verifyMonthPeriodStore(HdfsDataUnit df) {
        OUTPUT_FIELDS_NO_REDUCER = Arrays.asList(AccountId, PathPatternId, PeriodIdForPartition, Count,
                LastActivityDate);
        Object[][] expected = new Object[][]{
                {"2", "pp2", 237, 6, null}, //
                {"1", "pp1", 237, 8, null}, //
                {"2", "pp1", 237, 12, null}
        };
        verifyPeriodStore(expected, df, false);
        return false;
    }

    private Boolean verifyWeekPeriodStore(HdfsDataUnit df) {
        OUTPUT_FIELDS_NO_REDUCER = Arrays.asList(AccountId, PathPatternId, PeriodIdForPartition, Count,
                LastActivityDate);
        Object[][] expected = new Object[][]{
                {"2", "pp1", 1031, 5, null}, //
                {"2", "pp1", 1032, 7, null}, //
                {"2", "pp2", 1032, 6, null}, //
                {"1", "pp1", 1031, 8, null}
        };
        verifyPeriodStore(expected, df, false);
        return false;
    }

    private Boolean verifyReduced(HdfsDataUnit df) {
        OUTPUT_FIELDS_WITH_REDUCER = Arrays.asList(AccountId, OpportunityId, PeriodIdForPartition, StageName, Count,
                LastActivityDate);
        Object[][] expected = new Object[][]{ //
                {"acc2", "opp1", 982, "close", 1, 4L}, //
                {"acc2", "opp2", 983, "open", 1, 9L} //
        };
        verifyPeriodStore(expected, df, true);
        return false;
    }

    private void verifyPeriodStore(Object[][] expected, HdfsDataUnit df, boolean withReducer) {
        Map<Object, List<Object>> expectedMap = Arrays.stream(expected)
                .collect(Collectors.toMap(arr -> arr[0].toString() + arr[1].toString() + arr[2].toString(), Arrays::asList)); // accountId + pathPatternId/opportunityId + periodId
        Iterator<GenericRecord> iterator = verifyAndReadTarget(df);
        int rowCount = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iterator) {
            if (withReducer) {
                verifyTargetData(expectedMap, record, Arrays.asList(AccountId, OpportunityId, PeriodIdForPartition), OUTPUT_FIELDS_WITH_REDUCER);
            } else {
                verifyTargetData(expectedMap, record, Arrays.asList(AccountId, PathPatternId, PeriodIdForPartition), OUTPUT_FIELDS_NO_REDUCER);
            }
            rowCount++;
        }
        Assert.assertEquals(rowCount, expected.length);
    }

    private void verifyTargetData(Map<Object, List<Object>> expectedMap, GenericRecord record, List<String> keys, List<String> fields) {
        Assert.assertNotNull(record);
        Assert.assertTrue(keys.stream().noneMatch(key -> record.get(key) == null));
        String key = keys.stream().map(k -> record.get(k).toString()).collect(Collectors.joining(""));
        Assert.assertNotNull(expectedMap.get(key));
        List<Object> actual = fields.stream()
                .map(field -> record.get(field) == null ? null : record.get(field).toString())
                .collect(Collectors.toList());
        Assert.assertEquals(actual, expectedMap.get(key).stream().map(obj -> obj == null ? null : obj.toString())
                .collect(Collectors.toList()));
    }
}
