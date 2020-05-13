package com.latticeengines.spark.exposed.job.cdl;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LastActivityDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LastModifiedDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.OpportunityId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.StageName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.StageNameId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPattern;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPatternId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPatternName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.SourceMedium;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.SourceMediumId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.UserId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.WebVisitDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.WebVisitPageUrl;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.__Row_Count__;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.__StreamDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.__StreamDateId;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroupUtils;
import com.latticeengines.domain.exposed.cdl.activity.ActivityRowReducer;
import com.latticeengines.domain.exposed.cdl.activity.DimensionCalculator;
import com.latticeengines.domain.exposed.cdl.activity.DimensionCalculatorRegexMode;
import com.latticeengines.domain.exposed.cdl.activity.DimensionGenerator;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ActivityStoreSparkIOMetadata;
import com.latticeengines.domain.exposed.spark.cdl.AggDailyActivityConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class AggDailyActivityJobTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AggDailyActivityJobTestNG.class);

    private static final List<Pair<String, Class<?>>> RAW_STREAM_FIELDS = Arrays.asList( //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(UserId.name(), String.class), //
            Pair.of(SourceMedium.name(), String.class), //
            Pair.of(WebVisitPageUrl.name(), String.class), //
            Pair.of(__StreamDateId.name(), Integer.class), //
            Pair.of(__StreamDate.name(), String.class));
    private static final List<String> DAILY_AGG_OUTPUT_FIELDS = Arrays.asList(AccountId.name(), UserId.name(),
            SourceMediumId.name(), PathPatternId.name(), __StreamDate.name(), __Row_Count__.name(),
            LastActivityDate.name());
    private static final String STREAM_ID = "daily_agg_stream";
    private static final String DAY_0 = "2019-05-01"; // over retention days
    private static final String DAY_1 = "2019-07-01";
    private static final String DAY_2 = "2019-07-02";
    private static final String DAY_9 = "2019-07-09";
    private static final Integer DAY_PERIOD_0 = DateTimeUtils.dateToDayPeriod(DAY_0);
    private static final Integer DAY_PERIOD_1 = DateTimeUtils.dateToDayPeriod(DAY_1);
    private static final Integer DAY_PERIOD_2 = DateTimeUtils.dateToDayPeriod(DAY_2);
    private static final Integer DAY_PERIOD_9 = DateTimeUtils.dateToDayPeriod(DAY_9);
    private static final long DAY_0_EPOCH = 1556732005000L; // over retention days
    private static final long DAY_1_EPOCH = 1561964400000L;
    private static final long DAY_1_EPOCH_LATE = 1561964500000L;
    private static final long DAY_2_EPOCH = 1562050900000L;
    private static final long DAY_2_EPOCH_EARLY = 1562050800000L;
    private static final long DAY_3_EPOCH = 1562137200000L;
    private static final String ALL_CTN_PAGE_PTN_NAME = "all content pages";
    private static final String ALL_CTN_PAGE_PTN_HASH = DimensionGenerator.hashDimensionValue(ALL_CTN_PAGE_PTN_NAME);
    private static final String ALL_CTN_PAGE_PTN_ID = "1";
    private static final String VIDEO_CTN_PAGE_PTN_NAME = "all video content pages";
    private static final String VIDEO_CTN_PAGE_PTN_HASH = DimensionGenerator
            .hashDimensionValue(VIDEO_CTN_PAGE_PTN_NAME);
    private static final String VIDEO_CTN_PAGE_PTN_ID = "2";
    private static final String GOOGLE_PAID_SRC = "Google/Paid";
    private static final String GOOGLE_PAID_SRC_HASH = DimensionGenerator.hashDimensionValue(GOOGLE_PAID_SRC);
    private static final String GOOGLE_PAID_SRC_ID = "3";
    private static final String GOOGLE_ORGANIC_SRC = "Google/Organic";
    private static final String GOOGLE_ORGANIC_SRC_HASH = DimensionGenerator.hashDimensionValue(GOOGLE_ORGANIC_SRC);
    private static final String GOOGLE_ORGANIC_SRC_ID = "4";
    private static final String FACEBOOK_PAID_SRC = "Facebook/Paid";
    private static final String FACEBOOK_PAID_SRC_HASH = DimensionGenerator.hashDimensionValue(FACEBOOK_PAID_SRC);
    private static final String FACEBOOK_PAID_SRC_ID = "5";
    private static final String STAGE_WON = "won";
    private static final String STAGE_WON_ID = "1";
    private static final String STAGE_WON_HASH = DimensionGenerator.hashDimensionValue(STAGE_WON);
    private static final String STAGE_CLOSE = "close";
    private static final String STAGE_CLOSE_ID = "2";
    private static final String STAGE_CLOSE_HASH = DimensionGenerator.hashDimensionValue(STAGE_CLOSE);
    private static final String STAGE_NEW = "newStage";
    private static final String STAGE_NEW_ID = "3";
    private static final String STAGE_NEW_HASH = DimensionGenerator.hashDimensionValue(STAGE_NEW);
    private static final String STAGE_OLD = "oldStage";
    private static final String STAGE_OLD_ID = "4";
    private static final String STAGE_OLD_HASH = DimensionGenerator.hashDimensionValue(STAGE_OLD);

    private static final Map<String, String> WEBVISIT_DIMENSION_HASH_ID_MAP = new HashMap<>();
    private static final Map<String, String> OPPORTUNITY_DIMENSION_HASH_ID_MAP = new HashMap<>();

    static {
        WEBVISIT_DIMENSION_HASH_ID_MAP.put(ALL_CTN_PAGE_PTN_HASH, ALL_CTN_PAGE_PTN_ID);
        WEBVISIT_DIMENSION_HASH_ID_MAP.put(VIDEO_CTN_PAGE_PTN_HASH, VIDEO_CTN_PAGE_PTN_ID);
        WEBVISIT_DIMENSION_HASH_ID_MAP.put(GOOGLE_PAID_SRC_HASH, GOOGLE_PAID_SRC_ID);
        WEBVISIT_DIMENSION_HASH_ID_MAP.put(GOOGLE_ORGANIC_SRC_HASH, GOOGLE_ORGANIC_SRC_ID);
        WEBVISIT_DIMENSION_HASH_ID_MAP.put(FACEBOOK_PAID_SRC_HASH, FACEBOOK_PAID_SRC_ID);

        OPPORTUNITY_DIMENSION_HASH_ID_MAP.put(STAGE_WON_HASH, STAGE_WON_ID);
        OPPORTUNITY_DIMENSION_HASH_ID_MAP.put(STAGE_CLOSE_HASH, STAGE_CLOSE_ID);
        OPPORTUNITY_DIMENSION_HASH_ID_MAP.put(STAGE_NEW_HASH, STAGE_NEW_ID);
        OPPORTUNITY_DIMENSION_HASH_ID_MAP.put(STAGE_OLD_HASH, STAGE_OLD_ID);
    }

    @Test(groups = "functional")
    private void test() {
        AggDailyActivityConfig config = baseConfig();
        prepareTestData();
        log.info("Config = {}", JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(AggDailyActivityJob.class, config);
        log.info("Result = {}", JsonUtils.serialize(result));
        verifyResult(result);
    }

    @Test(groups = "functional")
    private void testIncrementalMode() {
        List<String> inputs = Arrays.asList(setupDeltaImport(), setupDailyBatchStore());
        AggDailyActivityConfig config = incrConfig(true);
        SparkJobResult result = runSparkJob(AggDailyActivityJob.class, config, inputs, getWorkspace());
        log.info("Output metadata: {}", result.getOutput());
        ActivityStoreSparkIOMetadata outputMetadata = JsonUtils.deserialize(result.getOutput(), ActivityStoreSparkIOMetadata.class);
        Assert.assertNotNull(outputMetadata);
        Assert.assertTrue(MapUtils.isNotEmpty(outputMetadata.getMetadata()));
        Assert.assertEquals(outputMetadata.getMetadata().size(), 1);
        Assert.assertEquals(result.getTargets().size(), 2);
    }

    @Test(groups = "functional")
    private void testIncrementalModeNoBatch() {
        List<String> inputs = Collections.singletonList(setupDeltaImport());
        AggDailyActivityConfig config = incrConfig(false);
        SparkJobResult result = runSparkJob(AggDailyActivityJob.class, config, inputs, getWorkspace());
        log.info("Output metadata: {}", result.getOutput());
        ActivityStoreSparkIOMetadata outputMetadata = JsonUtils.deserialize(result.getOutput(), ActivityStoreSparkIOMetadata.class);
        Assert.assertNotNull(outputMetadata);
        Assert.assertTrue(MapUtils.isNotEmpty(outputMetadata.getMetadata()));
        Assert.assertEquals(outputMetadata.getMetadata().size(), 1);
        Assert.assertEquals(result.getTargets().size(), 2);
    }

    @Test(groups = "functional")
    private void testIncrementalModeReducer() {
        List<String> inputs = Arrays.asList(setupReducerDeltaImport(), setupReducerBatchStore());
        AggDailyActivityConfig config = incrReducerConfig();
        SparkJobResult result = runSparkJob(AggDailyActivityJob.class, config, inputs, getWorkspace());
        log.info("Output metadata: {}", result.getOutput());
        ActivityStoreSparkIOMetadata outputMetadata = JsonUtils.deserialize(result.getOutput(), ActivityStoreSparkIOMetadata.class);
        Assert.assertNotNull(outputMetadata);
        Assert.assertTrue(MapUtils.isNotEmpty(outputMetadata.getMetadata()));
        Assert.assertEquals(outputMetadata.getMetadata().size(), 1);
        Assert.assertEquals(result.getTargets().size(), 2);
    }

    private AggDailyActivityConfig incrConfig(boolean withBatch) {
        AggDailyActivityConfig config = new AggDailyActivityConfig();
        ActivityStoreSparkIOMetadata inputMetadata = new ActivityStoreSparkIOMetadata();
        Map<String, ActivityStoreSparkIOMetadata.Details> detailsMap = new HashMap<>();
        ActivityStoreSparkIOMetadata.Details details = new ActivityStoreSparkIOMetadata.Details();
        details.setStartIdx(0);
        if (!withBatch) {
            details.setLabels(Collections.singletonList(ActivityMetricsGroupUtils.NO_BATCH));
        }
        detailsMap.put(STREAM_ID, details);
        inputMetadata.setMetadata(detailsMap);
        config.inputMetadata = inputMetadata;
        config.streamDateAttrs.put(STREAM_ID, WebVisitDate.name());
        config.dimensionMetadataMap.put(STREAM_ID, webVisitMetadata());
        config.dimensionCalculatorMap.put(STREAM_ID, webVisitDimensionCalculators());
        config.hashDimensionMap.put(STREAM_ID, Sets.newHashSet(SourceMediumId.name(), PathPatternId.name()));
        config.additionalDimAttrMap.put(STREAM_ID, Collections.singletonList(AccountId.name()));
        config.dimensionValueIdMap.putAll(WEBVISIT_DIMENSION_HASH_ID_MAP);
        config.incrementalStreams.add(STREAM_ID);

        return config;
    }

    private AggDailyActivityConfig incrReducerConfig() {
        AggDailyActivityConfig config = new AggDailyActivityConfig();
        ActivityStoreSparkIOMetadata inputMetadata = new ActivityStoreSparkIOMetadata();
        Map<String, ActivityStoreSparkIOMetadata.Details> detailsMap = new HashMap<>();
        ActivityStoreSparkIOMetadata.Details details = new ActivityStoreSparkIOMetadata.Details();
        details.setStartIdx(0);
        detailsMap.put(STREAM_ID, details);
        inputMetadata.setMetadata(detailsMap);
        config.inputMetadata = inputMetadata;
        config.streamDateAttrs.put(STREAM_ID, LastModifiedDate.name());
        config.dimensionMetadataMap.put(STREAM_ID, opportunityMetadata());
        config.dimensionCalculatorMap.put(STREAM_ID, opportunityDimensionCalculators());
        config.hashDimensionMap.put(STREAM_ID, Sets.newHashSet(StageNameId.name()));
        config.additionalDimAttrMap.put(STREAM_ID, Collections.singletonList(AccountId.name()));
        config.dimensionValueIdMap.putAll(OPPORTUNITY_DIMENSION_HASH_ID_MAP);
        config.incrementalStreams.add(STREAM_ID);
        config.streamReducerMap.put(STREAM_ID, prepareReducer());

        return config;
    }

    private ActivityRowReducer prepareReducer() {
        ActivityRowReducer reducer = new ActivityRowReducer();
        reducer.setGroupByFields(Collections.singletonList(OpportunityId.name()));
        reducer.setArguments(Collections.singletonList(LastModifiedDate.name()));
        reducer.setOperator(ActivityRowReducer.Operator.Latest);
        return reducer;
    }

    private String setupDailyBatchStore() {
        List<Pair<String, Class<?>>> inputFields = Arrays.asList( //
                Pair.of(PathPatternId.name(), Integer.class), //
                Pair.of(SourceMediumId.name(), Integer.class), //
                Pair.of(AccountId.name(), String.class), //
                Pair.of(__StreamDate.name(), String.class), //
                Pair.of(__StreamDateId.name(), Integer.class), //
                Pair.of(__Row_Count__.name(), Integer.class), //
                Pair.of(LastActivityDate.name(), Integer.class)
        );
        Object[][] data = new Object[][]{
                {1, 3, "a1", DAY_1, DAY_PERIOD_1, 2, DAY_PERIOD_1},
                {1, 3, "a1", DAY_2, DAY_PERIOD_2, 3, DAY_PERIOD_2},
                {2, 3, "a1", DAY_1, DAY_PERIOD_1, 1, DAY_PERIOD_1},
                {2, 3, "a9", DAY_9, DAY_PERIOD_9, 27, DAY_PERIOD_9}
        };
        return uploadHdfsDataUnit(data, inputFields);
    }

    private String setupDeltaImport() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(AccountId.name(), String.class), //
                Pair.of(SourceMedium.name(), String.class), //
                Pair.of(WebVisitPageUrl.name(), String.class), //
                Pair.of(WebVisitDate.name(), Long.class));
        Object[][] data = new Object[][]{
                {"a1", "Google/Paid", "https://dnb.com/contents/audios/1", DAY_3_EPOCH},
                {"a1", "Google/Paid", "https://dnb.com/contents/audios/1", DAY_1_EPOCH},
                {"a2", "Google/Paid", "https://dnb.com/contents/videos/2", DAY_2_EPOCH}
        };
        return uploadHdfsDataUnit(data, fields);
    }

    private String setupReducerDeltaImport() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(AccountId.name(), String.class), //
                Pair.of(OpportunityId.name(), String.class),
                Pair.of(StageName.name(), String.class),
                Pair.of(LastModifiedDate.name(), Long.class)
        );
        Object[][] data = new Object[][]{
                {"acc1", "opp1", STAGE_NEW, DAY_1_EPOCH_LATE}, // replacing record in batch
                {"acc2", "opp2", STAGE_OLD, DAY_2_EPOCH_EARLY}, // will be replaced by batch
        };
        return uploadHdfsDataUnit(data, fields);
    }

    private String setupReducerBatchStore() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(AccountId.name(), String.class), //
                Pair.of(OpportunityId.name(), String.class),
                Pair.of(StageName.name(), String.class),
                Pair.of(StageNameId.name(), String.class),
                Pair.of(LastModifiedDate.name(), Long.class),
                Pair.of(__StreamDate.name(), String.class),
                Pair.of(__StreamDateId.name(), Integer.class),
                Pair.of(__Row_Count__.name(), Integer.class),
                Pair.of(LastActivityDate.name(), Long.class)
        );
        Object[][] data = new Object[][]{
                {"acc1", "opp1", STAGE_WON, STAGE_WON_ID, DAY_1_EPOCH, DAY_1, DAY_PERIOD_1, 1, DAY_1_EPOCH},
                {"acc2", "opp2", STAGE_CLOSE, STAGE_CLOSE_ID, DAY_2_EPOCH, DAY_2, DAY_PERIOD_2, 1, DAY_2_EPOCH},
                {"acc1", "opp1", STAGE_OLD, STAGE_OLD_ID, DAY_0_EPOCH, DAY_0, DAY_PERIOD_0, 1, DAY_0_EPOCH} // should be removed by retention policy
        };
        return uploadHdfsDataUnit(data, fields);
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        AtomicInteger counter = new AtomicInteger(0);
        Map<String, Long> expectedRowCounts = getExpectedRowCounts();
        Map<String, Long> rowCounts = new HashMap<>();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            counter.incrementAndGet();
            log.info(debugStr(record, DAILY_AGG_OUTPUT_FIELDS));

            Pair<String, Long> result = getHashKeyAndRowCount(record);
            rowCounts.put(result.getKey(), result.getValue());
        });
        log.info("Number of records = {}", counter.get());
        Assert.assertEquals(rowCounts, expectedRowCounts);
        return true;
    }

    private Map<String, Long> getExpectedRowCounts() {
        Object[][] expectedResults = new Object[][]{ //
                {"a1", "u1", GOOGLE_PAID_SRC_ID, ALL_CTN_PAGE_PTN_ID, DAY_1, 2L}, //
                {"a1", "u1", GOOGLE_PAID_SRC_ID, VIDEO_CTN_PAGE_PTN_ID, DAY_1, 1L}, //
                {"a1", "u2", GOOGLE_PAID_SRC_ID, ALL_CTN_PAGE_PTN_ID, DAY_1, 1L}, //
                {"a1", "u2", GOOGLE_ORGANIC_SRC_ID, ALL_CTN_PAGE_PTN_ID, DAY_1, 1L}, //
                {"a1", "u2", GOOGLE_PAID_SRC_ID, VIDEO_CTN_PAGE_PTN_ID, DAY_1, 1L}, //
                {"a1", "u2", GOOGLE_ORGANIC_SRC_ID, VIDEO_CTN_PAGE_PTN_ID, DAY_1, 1L}, //
                {"a1", "u1", FACEBOOK_PAID_SRC_ID, ALL_CTN_PAGE_PTN_ID, DAY_1, 1L}, //
                {"a1", "u1", FACEBOOK_PAID_SRC_ID, VIDEO_CTN_PAGE_PTN_ID, DAY_1, 1L}, //
                {"a1", "u1", GOOGLE_PAID_SRC_ID, ALL_CTN_PAGE_PTN_ID, DAY_2, 3L}, //
                /*-
                 * source null
                 */
                {"a1", "u1", null, ALL_CTN_PAGE_PTN_ID, DAY_2, 2L}, //
                {"a1", "u1", null, VIDEO_CTN_PAGE_PTN_ID, DAY_2, 1L}, //
                {"a1", "u2", null, ALL_CTN_PAGE_PTN_ID, DAY_1, 2L}, //
                {"a1", "u2", null, ALL_CTN_PAGE_PTN_ID, DAY_2, 3L}, //
                {"a1", "u2", null, VIDEO_CTN_PAGE_PTN_ID, DAY_2, 2L}, //
                /*-
                 * url null
                 */
                {"a1", "u1", GOOGLE_PAID_SRC_ID, null, DAY_2, 2L}, //
                {"a1", "u1", FACEBOOK_PAID_SRC_ID, null, DAY_2, 1L}, //
                /*-
                 * both null
                 */
                {"a1", "u1", null, null, DAY_1, 3L}, //
                {"a1", "u2", null, null, DAY_1, 2L}, //
                {"a1", "u1", null, null, DAY_2, 1L}, //

        };
        return Arrays.stream(expectedResults) //
                .map(this::getHashKeyAndRowCount) //
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    private Pair<String, Long> getHashKeyAndRowCount(GenericRecord record) {
        String accId = getStr(record.get(AccountId.name()));
        String userId = getStr(record.get(UserId.name()));
        String smId = getStr(record.get(SourceMediumId.name()));
        String ptnId = getStr(record.get(PathPatternId.name()));
        String date = getStr(record.get(__StreamDate.name()));
        Long count = (Long) record.get(__Row_Count__.name());
        return Pair.of(hashKey(accId, userId, smId, ptnId, date), count);
    }

    private Pair<String, Long> getHashKeyAndRowCount(Object[] row) {
        String accId = (String) row[DAILY_AGG_OUTPUT_FIELDS.indexOf(AccountId.name())];
        String userId = (String) row[DAILY_AGG_OUTPUT_FIELDS.indexOf(UserId.name())];
        String smId = (String) row[DAILY_AGG_OUTPUT_FIELDS.indexOf(SourceMediumId.name())];
        String ptnId = (String) row[DAILY_AGG_OUTPUT_FIELDS.indexOf(PathPatternId.name())];
        String date = (String) row[DAILY_AGG_OUTPUT_FIELDS.indexOf(__StreamDate.name())];
        String rowCountStr = row[DAILY_AGG_OUTPUT_FIELDS.indexOf(__Row_Count__.name())].toString();
        return Pair.of(hashKey(accId, userId, smId, ptnId, date), Long.parseLong(rowCountStr));
    }

    private String getStr(Object obj) {
        return obj == null ? null : obj.toString();
    }

    private String hashKey(String accId, String userId, String smId, String ptnId, String dateStr) {
        return String.join("_", accId, userId, smId, ptnId, dateStr);
    }

    private void prepareTestData() {
        Object[][] data = new Object[][]{ //
                /*-
                 * both url & source match
                 * 1. a1,u1,Google/Paid,all_content_pages,Day1 => row=2
                 * 2. a1,u1,Google/Paid,all_video_content_pages,Day1 => row=1
                 * 3. a1,u2,Google/Paid,all_content_pages,Day1 => row=1
                 * 4. a1,u2,Google/Organic,all_content_pages,Day1 => row=1
                 * 5. a1,u2,Google/Paid,all_video_content_pages,Day1 => row=1
                 * 6. a1,u2,Google/Organic,all_video_content_pages,Day1 => row=1
                 * 7. a1,u1,Facebook/Paid,all_content_pages,Day1 => row=1
                 * 8. a1,u1,Facebook/Paid,all_video_content_pages,Day1 => row=1
                 * 9. a1,u1,Google/Paid,all_content_pages,Day2 => row=3
                 */
                {"a1", "u1", "Google/Paid", "https://dnb.com/contents/audios/1", DAY_PERIOD_1, DAY_1}, //
                {"a1", "u1", "Facebook/Paid", "https://dnb.com/contents/videos/1", DAY_PERIOD_1, DAY_1}, //
                {"a1", "u1", "Google/Paid", "https://dnb.com/contents/videos/2", DAY_PERIOD_1, DAY_1}, //
                {"a1", "u2", "Google/Organic", "https://dnb.com/contents/videos/2", DAY_PERIOD_1, DAY_1}, //
                {"a1", "u2", "Google/Paid", "https://dnb.com/contents/videos/2", DAY_PERIOD_1, DAY_1}, //
                {"a1", "u1", "Google/Paid", "https://dnb.com/contents/audios/1", DAY_PERIOD_2, DAY_2}, //
                {"a1", "u1", "Google/Paid", "https://dnb.com/contents/audios/3", DAY_PERIOD_2, DAY_2}, //
                {"a1", "u1", "Google/Paid", "https://dnb.com/contents/audios/5", DAY_PERIOD_2, DAY_2}, //
                /*-
                 * only source not match
                 * 1. a1,u1,null,all_content_pages,Day2 => row=2
                 * 2. a1,u1,null,all_video_content_pages,Day2 => row=1
                 * 3. a1,u2,null,all_content_pages,Day1 => row=2
                 * 4. a1,u2,null,all_content_pages,Day2 => row=3
                 * 5. a1,u2,null,all_video_content_pages,Day2 => row=2
                 */
                {"a1", "u1", "Netflix/Paid", "https://dnb.com/contents/videos/4", DAY_PERIOD_2, DAY_2}, //
                {"a1", "u1", "", "https://dnb.com/contents/audio/5", DAY_PERIOD_2, DAY_2}, //
                {"a1", "u2", null, "https://dnb.com/contents/images/3", DAY_PERIOD_1, DAY_1}, //
                {"a1", "u2", "sdkljflsjk", "https://dnb.com/contents/audios/5", DAY_PERIOD_1, DAY_1}, //
                {"a1", "u2", "sdkljflsjk", "https://dnb.com/contents/audios/5", DAY_PERIOD_2, DAY_2}, //
                {"a1", "u2", "", "https://dnb.com/contents/videos/6", DAY_PERIOD_2, DAY_2}, //
                {"a1", "u2", "", "https://dnb.com/contents/videos/9", DAY_PERIOD_2, DAY_2}, //
                /*-
                 * only url not match
                 * 1. a1,u1,Google/Paid,null,Day2 => row=2
                 * 2. a1,u1,Facebook/Paid,null,Day2 => row=1
                 */
                {"a1", "u1", "Google/Paid", "https://dnb.com/users/5", DAY_PERIOD_2, DAY_2}, //
                {"a1", "u1", "Facebook/Paid", "https://dnb.com/users/4", DAY_PERIOD_2, DAY_2}, //
                {"a1", "u1", "Google/Paid", "https://dnb.com/users/3", DAY_PERIOD_2, DAY_2}, //
                /*-
                 * both url & source not match any value in dimension value space
                 * 1. a1,u1,null,null,Day1 => row=3
                 * 2. a1,u2,null,null,Day1 => row=2
                 * 3. a1,u1,null,null,Day2 => row=1
                 */
                {"a1", "u1", "Netflix/Paid", null, DAY_PERIOD_1, DAY_1}, //
                {"a1", "u1", "", "/test", DAY_PERIOD_1, DAY_1}, //
                {"a1", "u1", "", "", DAY_PERIOD_1, DAY_1}, //
                {"a1", "u2", null, null, DAY_PERIOD_1, DAY_1}, //
                {"a1", "u2", null, "/hello", DAY_PERIOD_1, DAY_1}, //
                {"a1", "u1", "Netflix/Paid", "/hello", DAY_PERIOD_2, DAY_2}, //
        };
        uploadHdfsDataUnit(data, RAW_STREAM_FIELDS);
    }

    private AggDailyActivityConfig baseConfig() {
        AggDailyActivityConfig config = new AggDailyActivityConfig();
        ActivityStoreSparkIOMetadata inputMetadata = new ActivityStoreSparkIOMetadata();
        Map<String, ActivityStoreSparkIOMetadata.Details> detailsMap = new HashMap<>();
        ActivityStoreSparkIOMetadata.Details details = new ActivityStoreSparkIOMetadata.Details();
        details.setStartIdx(0);
        detailsMap.put(STREAM_ID, details);
        inputMetadata.setMetadata(detailsMap);
        config.inputMetadata = inputMetadata;
        config.streamDateAttrs.put(STREAM_ID, __StreamDateId.name());
        config.dimensionMetadataMap.put(STREAM_ID, webVisitMetadata());
        config.dimensionCalculatorMap.put(STREAM_ID, webVisitDimensionCalculators());
        config.hashDimensionMap.put(STREAM_ID, Sets.newHashSet(SourceMediumId.name(), PathPatternId.name()));
        config.additionalDimAttrMap.put(STREAM_ID, Arrays.asList(AccountId.name(), UserId.name()));
        config.dimensionValueIdMap.putAll(WEBVISIT_DIMENSION_HASH_ID_MAP);
        return config;
    }

    private Map<String, DimensionMetadata> webVisitMetadata() {
        Map<String, DimensionMetadata> metadataMap = new HashMap<>();
        metadataMap.put(PathPatternId.name(), ptnMetadata());
        metadataMap.put(SourceMediumId.name(), smMetadata());
        return metadataMap;
    }

    private Map<String, DimensionMetadata> opportunityMetadata() {
        Map<String, DimensionMetadata> metadataMap = new HashMap<>();
        metadataMap.put(StageNameId.name(), stageMetadata());
        return metadataMap;
    }

    private DimensionMetadata smMetadata() {
        DimensionMetadata metadata = new DimensionMetadata();
        metadata.setDimensionValues(
                Arrays.asList(smValue(GOOGLE_PAID_SRC), smValue(FACEBOOK_PAID_SRC), smValue(GOOGLE_ORGANIC_SRC)));
        metadata.setCardinality(3);
        return metadata;
    }

    private DimensionMetadata stageMetadata() {
        DimensionMetadata metadata = new DimensionMetadata();
        metadata.setDimensionValues(
                Arrays.asList(stageValue(STAGE_WON), stageValue(STAGE_CLOSE), stageValue(STAGE_NEW), stageValue(STAGE_OLD))
        );
        metadata.setCardinality(4);
        return metadata;
    }

    private Map<String, Object> smValue(String srcMedium) {
        Map<String, Object> values = new HashMap<>();
        values.put(SourceMedium.name(), srcMedium);
        values.put(SourceMediumId.name(), WEBVISIT_DIMENSION_HASH_ID_MAP.get(DimensionGenerator.hashDimensionValue(srcMedium)));
        return values;
    }

    private Map<String, Object> stageValue(String stage) {
        Map<String, Object> values = new HashMap<>();
        values.put(StageName.name(), stage);
        values.put(StageNameId.name(), OPPORTUNITY_DIMENSION_HASH_ID_MAP.get(DimensionGenerator.hashDimensionValue(stage)));
        return values;
    }

    private DimensionMetadata ptnMetadata() {
        DimensionMetadata metadata = new DimensionMetadata();
        Map<String, Object> content = pathPtnValue("*dnb.com/contents/*", ALL_CTN_PAGE_PTN_NAME);
        Map<String, Object> video = pathPtnValue("*dnb.com/contents/videos/*", VIDEO_CTN_PAGE_PTN_NAME);
        metadata.setDimensionValues(Arrays.asList(content, video));
        metadata.setCardinality(2);
        return metadata;
    }

    private Map<String, Object> pathPtnValue(String pathPattern, String pathPatternName) {
        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put(PathPatternId.name(),
                WEBVISIT_DIMENSION_HASH_ID_MAP.get(DimensionGenerator.hashDimensionValue(pathPatternName)));
        valueMap.put(PathPatternName.name(), pathPatternName);
        valueMap.put(PathPattern.name(), pathPattern);
        return valueMap;
    }

    private Map<String, DimensionCalculator> webVisitDimensionCalculators() {
        Map<String, DimensionCalculator> calculatorMap = new HashMap<>();
        DimensionCalculatorRegexMode ptnCalculator = new DimensionCalculatorRegexMode();
        ptnCalculator.setName(InterfaceName.WebVisitPageUrl.name());
        ptnCalculator.setAttribute(InterfaceName.WebVisitPageUrl.name());
        ptnCalculator.setPatternAttribute(InterfaceName.PathPattern.name());
        ptnCalculator.setPatternFromCatalog(true);
        calculatorMap.put(PathPatternId.name(), ptnCalculator);

        DimensionCalculator smCalculator = new DimensionCalculator();
        smCalculator.setName(InterfaceName.SourceMedium.name());
        smCalculator.setAttribute(InterfaceName.SourceMedium.name());
        calculatorMap.put(SourceMediumId.name(), smCalculator);
        return calculatorMap;
    }

    private Map<String, DimensionCalculator> opportunityDimensionCalculators() {
        Map<String, DimensionCalculator> calculatorMap = new HashMap<>();
        DimensionCalculator stageCalculator = new DimensionCalculator();
        stageCalculator.setName(StageName.name());
        stageCalculator.setAttribute(StageName.name());
        calculatorMap.put(StageNameId.name(), stageCalculator);
        return calculatorMap;
    }
}
