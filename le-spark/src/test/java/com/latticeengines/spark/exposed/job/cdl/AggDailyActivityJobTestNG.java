package com.latticeengines.spark.exposed.job.cdl;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Amount;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Cost;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.DerivedId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LastActivityDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LastModifiedDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ModelName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.OpportunityId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPattern;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPatternId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPatternName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ProductId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ProductType;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Quantity;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.SourceMedium;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.SourceMediumId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.StageName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.StageNameId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.StreamDateId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.TransactionId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.TransactionTime;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.TransactionType;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.UserId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.WebVisitDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.WebVisitPageUrl;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.__Row_Count__;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.__StreamDate;

import java.util.ArrayList;
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
import com.latticeengines.domain.exposed.cdl.activity.CompositeDimensionCalculator;
import com.latticeengines.domain.exposed.cdl.activity.DeriveConfig;
import com.latticeengines.domain.exposed.cdl.activity.DimensionCalculator;
import com.latticeengines.domain.exposed.cdl.activity.DimensionCalculatorRegexMode;
import com.latticeengines.domain.exposed.cdl.activity.DimensionGenerator;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.AggDailyActivityConfig;
import com.latticeengines.domain.exposed.spark.cdl.SparkIOMetadataWrapper;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;
import com.latticeengines.spark.util.DeriveAttrsUtils;

public class AggDailyActivityJobTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AggDailyActivityJobTestNG.class);

    private static final List<Pair<String, Class<?>>> RAW_STREAM_FIELDS = Arrays.asList( //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(UserId.name(), String.class), //
            Pair.of(SourceMedium.name(), String.class), //
            Pair.of(WebVisitPageUrl.name(), String.class), //
            Pair.of(StreamDateId.name(), Integer.class), //
            Pair.of(__StreamDate.name(), String.class));
    private static final List<String> DAILY_AGG_OUTPUT_FIELDS = Arrays.asList(AccountId.name(), UserId.name(),
            SourceMediumId.name(), PathPatternId.name(), __StreamDate.name(), __Row_Count__.name(),
            LastActivityDate.name(), DeriveAttrsUtils.VERSION_COL());
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
    private static final long DAY_9_EPOCH = 1562737007000L;
    private static final long OLD_VERSION = 10L;
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
    private static final String MODEL_1 = "m1";
    private static final String MODEL_1_ID = "1";
    private static final String MODEL_1_HASH = DimensionGenerator.hashDimensionValue(MODEL_1);
    private static final String MODEL_2 = "m2";
    private static final String MODEL_2_ID = "2";
    private static final String MODEL_2_HASH = DimensionGenerator.hashDimensionValue(MODEL_2);
    private static final String DERIVED_NAME_1 = "derived Closed Won B1";
    private static final String DERIVED_NAME_1_HASH = DimensionGenerator.hashDimensionValue(DERIVED_NAME_1);
    private static final String DERIVED_NAME_1_ID = "11";
    private static final String DERIVED_NAME_2 = "derived Closed Won B2";
    private static final String DERIVED_NAME_2_HASH = DimensionGenerator.hashDimensionValue(DERIVED_NAME_2);
    private static final String DERIVED_NAME_2_ID = "12";

    private static final Map<String, String> WEBVISIT_DIMENSION_HASH_ID_MAP = new HashMap<>();
    private static final Map<String, String> OPPORTUNITY_DIMENSION_HASH_ID_MAP = new HashMap<>();
    private static final Map<String, String> INTENT_DIMENSION_HASH_ID_MAP = new HashMap<>();

    private static final String VERSION_COL = DeriveAttrsUtils.VERSION_COL();

    private static final String modelName = InterfaceName.ModelName.name();
    private static final String modelNameId = InterfaceName.ModelNameId.name();
    private static final String Date = "Date";
    private static final String hasIntent = InterfaceName.HasIntent.name();
    private static final String BuyingScore = "buyingScore";
    private static final String BUYING = "buying";
    private static final String RESEARCHING = "researching";

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
        OPPORTUNITY_DIMENSION_HASH_ID_MAP.put(DERIVED_NAME_1_HASH, DERIVED_NAME_1_ID);
        OPPORTUNITY_DIMENSION_HASH_ID_MAP.put(DERIVED_NAME_2_HASH, DERIVED_NAME_2_ID);

        INTENT_DIMENSION_HASH_ID_MAP.put(MODEL_1_HASH, MODEL_1_ID);
        INTENT_DIMENSION_HASH_ID_MAP.put(MODEL_2_HASH, MODEL_2_ID);
    }

    @Test(groups = "functional")
    private void test() {
        AggDailyActivityConfig config = baseConfig();
        prepareTestData();
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
        SparkIOMetadataWrapper outputMetadata = JsonUtils.deserialize(result.getOutput(), SparkIOMetadataWrapper.class);
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
        SparkIOMetadataWrapper outputMetadata = JsonUtils.deserialize(result.getOutput(), SparkIOMetadataWrapper.class);
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
        SparkIOMetadataWrapper outputMetadata = JsonUtils.deserialize(result.getOutput(), SparkIOMetadataWrapper.class);
        Assert.assertNotNull(outputMetadata);
        Assert.assertTrue(MapUtils.isNotEmpty(outputMetadata.getMetadata()));
        Assert.assertEquals(outputMetadata.getMetadata().size(), 1);
        Assert.assertEquals(result.getTargets().size(), 2);
    }

    @Test(groups = "functional")
    private void testIntentActivity() {
        List<String> inputs = Collections.singletonList(setupIntentActivityRawStreamData());
        AggDailyActivityConfig config = intentBaseConfig();
        SparkJobResult result = runSparkJob(AggDailyActivityJob.class, config, inputs, getWorkspace());
        log.info("Output metadata: {}", result.getOutput());
    }

    @Test(groups = "functional")
    private void testIntentIncr() {
        List<String> inputs = Arrays.asList(setupIntentDeltaImport(), setupIntentBatchStore());
        AggDailyActivityConfig config = intentIncrConfig();
        SparkJobResult result = runSparkJob(AggDailyActivityJob.class, config, inputs, getWorkspace());
        log.info("Output metadata: {}", result.getOutput());
    }

    @Test(groups = "functional")
    private void testBuyingScore() {
        List<String> inputs = Collections.singletonList(setupIntentActivityRawStreamData());
        AggDailyActivityConfig config = buyingScoreConfig();
        SparkJobResult result = runSparkJob(AggDailyActivityJob.class, config, inputs, getWorkspace());
        log.info("Output metadata: {}", result.getOutput());
    }

    @Test(groups = "functional")
    private void testBuyingScoreIncr() {
        List<String> inputs = Arrays.asList(setupBuyingScoreDeltaImport(), setupBuyingScoreBatchStore());
        AggDailyActivityConfig config = buyingScoreIncrConfig();
        SparkJobResult result = runSparkJob(AggDailyActivityJob.class, config, inputs, getWorkspace());
        log.info("Output metadata: {}", result.getOutput());
    }

    @Test(groups = "functional")
    private void testTransaction() {
        List<String> inputs = Collections.singletonList(setupRawTransactionStreamData());
        AggDailyActivityConfig config = txnConfig();
        SparkJobResult result = runSparkJob(AggDailyActivityJob.class, config, inputs, getWorkspace());
        log.info("Output metadata: {}", result.getOutput());
    }

    @Test(groups = "functional")
    private void testDeriveDimension() {
        List<String> inputs = Collections.singletonList(setupRawStreamWithDerivedDimension());
        AggDailyActivityConfig config = opportunityDeriveDimConfig();
        SparkJobResult result = runSparkJob(AggDailyActivityJob.class, config, inputs, getWorkspace());
        log.info("Output metadata: {}", result.getOutput());
        SparkIOMetadataWrapper outputMetadata = JsonUtils.deserialize(result.getOutput(), SparkIOMetadataWrapper.class);
    }

    private AggDailyActivityConfig incrConfig(boolean withBatch) {
        AggDailyActivityConfig config = new AggDailyActivityConfig();
        SparkIOMetadataWrapper inputMetadata = new SparkIOMetadataWrapper();
        Map<String, SparkIOMetadataWrapper.Partition> detailsMap = new HashMap<>();
        SparkIOMetadataWrapper.Partition details = new SparkIOMetadataWrapper.Partition();
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
        config.currentEpochMilli = DAY_0_EPOCH;

        return config;
    }

    private AggDailyActivityConfig opportunityDeriveDimConfig() {
        AggDailyActivityConfig config = new AggDailyActivityConfig();
        SparkIOMetadataWrapper inputMetadata = new SparkIOMetadataWrapper();
        Map<String, SparkIOMetadataWrapper.Partition> detailsMap = new HashMap<>();
        SparkIOMetadataWrapper.Partition details = new SparkIOMetadataWrapper.Partition();
        details.setStartIdx(0);
        detailsMap.put(STREAM_ID, details);
        inputMetadata.setMetadata(detailsMap);
        config.inputMetadata = inputMetadata;
        config.streamDateAttrs.put(STREAM_ID, LastModifiedDate.name());
        config.dimensionMetadataMap.put(STREAM_ID, opportunityMetadataWithDerivedDimension());
        config.dimensionCalculatorMap.put(STREAM_ID, opportunityDimensionCalculatorsWithDerivedDimension());
        config.hashDimensionMap.put(STREAM_ID, Sets.newHashSet(StageNameId.name(), DerivedId.name()));
        config.additionalDimAttrMap.put(STREAM_ID, Collections.singletonList(AccountId.name()));
        config.dimensionValueIdMap.putAll(OPPORTUNITY_DIMENSION_HASH_ID_MAP);
        config.streamReducerMap.put(STREAM_ID, prepareReducer());
        config.currentEpochMilli = DAY_0_EPOCH;

        return config;
    }

    private AggDailyActivityConfig incrReducerConfig() {
        AggDailyActivityConfig config = new AggDailyActivityConfig();
        SparkIOMetadataWrapper inputMetadata = new SparkIOMetadataWrapper();
        Map<String, SparkIOMetadataWrapper.Partition> detailsMap = new HashMap<>();
        SparkIOMetadataWrapper.Partition details = new SparkIOMetadataWrapper.Partition();
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
        config.currentEpochMilli = DAY_0_EPOCH;

        return config;
    }

    private AggDailyActivityConfig intentBaseConfig() {
        AggDailyActivityConfig config = new AggDailyActivityConfig();
        SparkIOMetadataWrapper inputMetadata = new SparkIOMetadataWrapper();
        Map<String, SparkIOMetadataWrapper.Partition> detailsMap = new HashMap<>();
        SparkIOMetadataWrapper.Partition details = new SparkIOMetadataWrapper.Partition();
        details.setStartIdx(0);
        detailsMap.put(STREAM_ID, details);
        inputMetadata.setMetadata(detailsMap);
        config.inputMetadata = inputMetadata;
        config.streamDateAttrs.put(STREAM_ID, Date);
        config.dimensionMetadataMap.put(STREAM_ID, intentMetadata());
        config.dimensionCalculatorMap.put(STREAM_ID, intentDimensionCalculators());
        config.hashDimensionMap.put(STREAM_ID, Sets.newHashSet(modelNameId));
        config.additionalDimAttrMap.put(STREAM_ID, Collections.singletonList(AccountId.name()));
        config.dimensionValueIdMap.putAll(INTENT_DIMENSION_HASH_ID_MAP);
        config.currentEpochMilli = DAY_0_EPOCH;
        return config;
    }

    private AggDailyActivityConfig intentIncrConfig() {
        AggDailyActivityConfig config = new AggDailyActivityConfig();
        SparkIOMetadataWrapper inputMetadata = new SparkIOMetadataWrapper();
        Map<String, SparkIOMetadataWrapper.Partition> detailsMap = new HashMap<>();
        SparkIOMetadataWrapper.Partition details = new SparkIOMetadataWrapper.Partition();
        details.setStartIdx(0);
        detailsMap.put(STREAM_ID, details);
        inputMetadata.setMetadata(detailsMap);
        config.inputMetadata = inputMetadata;
        config.streamDateAttrs.put(STREAM_ID, Date);
        config.dimensionMetadataMap.put(STREAM_ID, intentMetadata());
        config.dimensionCalculatorMap.put(STREAM_ID, intentDimensionCalculators());
        config.hashDimensionMap.put(STREAM_ID, Sets.newHashSet(modelNameId));
        config.additionalDimAttrMap.put(STREAM_ID, Collections.singletonList(AccountId.name()));
        config.dimensionValueIdMap.putAll(INTENT_DIMENSION_HASH_ID_MAP);
        config.incrementalStreams.add(STREAM_ID);
        config.currentEpochMilli = DAY_0_EPOCH;
        return config;
    }

    private AggDailyActivityConfig buyingScoreConfig() {
        AggDailyActivityConfig config = new AggDailyActivityConfig();
        SparkIOMetadataWrapper inputMetadata = new SparkIOMetadataWrapper();
        Map<String, SparkIOMetadataWrapper.Partition> detailsMap = new HashMap<>();
        SparkIOMetadataWrapper.Partition details = new SparkIOMetadataWrapper.Partition();
        details.setStartIdx(0);
        detailsMap.put(STREAM_ID, details);
        inputMetadata.setMetadata(detailsMap);
        config.inputMetadata = inputMetadata;
        config.streamDateAttrs.put(STREAM_ID, Date);
        config.dimensionMetadataMap.put(STREAM_ID, intentMetadata());
        config.dimensionCalculatorMap.put(STREAM_ID, intentDimensionCalculators());
        config.hashDimensionMap.put(STREAM_ID, Sets.newHashSet(modelNameId));
        config.additionalDimAttrMap.put(STREAM_ID, Collections.singletonList(AccountId.name()));
        config.dimensionValueIdMap.putAll(INTENT_DIMENSION_HASH_ID_MAP);
        config.currentEpochMilli = DAY_0_EPOCH;
        config.streamReducerMap.put(STREAM_ID, prepareBuyingScoreReducer());
        return config;
    }

    private AggDailyActivityConfig buyingScoreIncrConfig() {
        AggDailyActivityConfig config = new AggDailyActivityConfig();
        SparkIOMetadataWrapper inputMetadata = new SparkIOMetadataWrapper();
        Map<String, SparkIOMetadataWrapper.Partition> detailsMap = new HashMap<>();
        SparkIOMetadataWrapper.Partition details = new SparkIOMetadataWrapper.Partition();
        details.setStartIdx(0);
        detailsMap.put(STREAM_ID, details);
        inputMetadata.setMetadata(detailsMap);
        config.inputMetadata = inputMetadata;
        config.streamDateAttrs.put(STREAM_ID, Date);
        config.dimensionMetadataMap.put(STREAM_ID, intentMetadata());
        config.dimensionCalculatorMap.put(STREAM_ID, intentDimensionCalculators());
        config.hashDimensionMap.put(STREAM_ID, Sets.newHashSet(modelNameId));
        config.additionalDimAttrMap.put(STREAM_ID, Collections.singletonList(AccountId.name()));
        config.dimensionValueIdMap.putAll(INTENT_DIMENSION_HASH_ID_MAP);
        config.incrementalStreams.add(STREAM_ID);
        config.streamReducerMap.put(STREAM_ID, prepareBuyingScoreReducer());
        config.currentEpochMilli = DAY_0_EPOCH;
        return config;
    }

    private AggDailyActivityConfig txnConfig() {
        AggDailyActivityConfig config = new AggDailyActivityConfig();
        config.streamDateAttrs.put(STREAM_ID, InterfaceName.TransactionTime.name());
        config.attrDeriverMap.put(STREAM_ID, prepareTxnDerivers());
        config.additionalDimAttrMap.put(STREAM_ID, Arrays.asList( //
                InterfaceName.AccountId.name(), //
                InterfaceName.ContactId.name(), //
                InterfaceName.ProductId.name(), //
                TransactionType.name(), //
                ProductType.name()
        ));
        config.currentEpochMilli = DAY_0_EPOCH;
        SparkIOMetadataWrapper inputMetadata = new SparkIOMetadataWrapper();
        Map<String, SparkIOMetadataWrapper.Partition> detailsMap = new HashMap<>();
        SparkIOMetadataWrapper.Partition details = new SparkIOMetadataWrapper.Partition();
        details.setStartIdx(0);
        detailsMap.put(STREAM_ID, details);
        inputMetadata.setMetadata(detailsMap);
        config.inputMetadata = inputMetadata;
        return config;
    }

    private List<StreamAttributeDeriver> prepareTxnDerivers() {
        return Arrays.asList( //
                constructSumDeriver(InterfaceName.Amount.name()), //
                constructSumDeriver(InterfaceName.Quantity.name()), //
                constructSumDeriver(InterfaceName.Cost.name()) //
        );
    }

    private StreamAttributeDeriver constructSumDeriver(String attrName) {
        StreamAttributeDeriver deriver = new StreamAttributeDeriver();
        deriver.setSourceAttributes(Collections.singletonList(attrName));
        deriver.setTargetAttribute(attrName);
        deriver.setCalculation(StreamAttributeDeriver.Calculation.SUM);
        return deriver;
    }

    private ActivityRowReducer prepareReducer() {
        ActivityRowReducer reducer = new ActivityRowReducer();
        reducer.setGroupByFields(Collections.singletonList(OpportunityId.name()));
        reducer.setArguments(Collections.singletonList(LastModifiedDate.name()));
        reducer.setOperator(ActivityRowReducer.Operator.Latest);
        return reducer;
    }

    private ActivityRowReducer prepareBuyingScoreReducer() {
        ActivityRowReducer reducer = new ActivityRowReducer();
        reducer.setGroupByFields(Arrays.asList(AccountId.name(), ModelName.name()));
        reducer.setArguments(Collections.singletonList(Date));
        reducer.setOperator(ActivityRowReducer.Operator.Latest);
        return reducer;
    }

    private String setupDailyBatchStore() {
        List<Pair<String, Class<?>>> inputFields = Arrays.asList( //
                Pair.of(PathPatternId.name(), Integer.class), //
                Pair.of(SourceMediumId.name(), Integer.class), //
                Pair.of(AccountId.name(), String.class), //
                Pair.of(__StreamDate.name(), String.class), //
                Pair.of(StreamDateId.name(), Integer.class), //
                Pair.of(__Row_Count__.name(), Integer.class), //
                Pair.of(LastActivityDate.name(), Long.class)
        );
        Object[][] data = new Object[][]{
                {1, 3, "a1", DAY_1, DAY_PERIOD_1, 2, DAY_1_EPOCH},
                {1, 3, "a1", DAY_2, DAY_PERIOD_2, 3, DAY_2_EPOCH},
                {2, 3, "a1", DAY_1, DAY_PERIOD_1, 1, DAY_1_EPOCH},
                {2, 3, "a9", DAY_9, DAY_PERIOD_9, 27, DAY_9_EPOCH}
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

    private String setupRawStreamWithDerivedDimension() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(AccountId.name(), String.class), //
                Pair.of(UserId.name(), String.class), // simulating BU
                Pair.of(OpportunityId.name(), String.class), //
                Pair.of(StageName.name(), String.class), //
                Pair.of(LastModifiedDate.name(), Long.class), //
                Pair.of(__StreamDate.name(), String.class), //
                Pair.of(StreamDateId.name(), Integer.class) //
        );
        Object[][] data = new Object[][]{
                {"acc1", "bu1", "opp1", STAGE_WON, DAY_1_EPOCH, DAY_1, DAY_PERIOD_1}, // derived Closed Won B1
                {"acc2", "bu2", "opp2", STAGE_WON, DAY_1_EPOCH_LATE, DAY_1, DAY_PERIOD_1} // derived Closed Won B2
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
                Pair.of(StreamDateId.name(), Integer.class),
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

    private String setupIntentDeltaImport() {
        List<Pair<String, Class<?>>> fields = Arrays.asList(
                Pair.of(AccountId.name(), String.class),
                Pair.of(modelName, String.class),
                Pair.of(Date, Long.class),
                Pair.of(LastActivityDate.name(), Long.class)
        );
        Object[][] data = new Object[][]{
                {"acc1", MODEL_1, DAY_1_EPOCH, DAY_1_EPOCH},
                {"acc2", MODEL_1, DAY_1_EPOCH, DAY_1_EPOCH},
                {"acc1", MODEL_2, DAY_1_EPOCH, DAY_1_EPOCH},
                {"acc1", MODEL_1, DAY_2_EPOCH, DAY_2_EPOCH},
                {"acc999", MODEL_1, DAY_1_EPOCH, DAY_1_EPOCH}
        };
        return uploadHdfsDataUnit(data, fields);
    }

    private String setupIntentBatchStore() {
        List<Pair<String, Class<?>>> fields = Arrays.asList(
                Pair.of(AccountId.name(), String.class),
                Pair.of(modelNameId, String.class),
                Pair.of(__StreamDate.name(), String.class),
                Pair.of(StreamDateId.name(), Integer.class),
                Pair.of(__Row_Count__.name(), Integer.class),
                Pair.of(LastActivityDate.name(), Long.class),
                Pair.of(VERSION_COL, Long.class)
        );
        Object[][] data = new Object[][]{
                {"acc1", MODEL_1_ID, DAY_1, DAY_PERIOD_1, 12, 0L, OLD_VERSION}, // replaced with new version stamp
                {"acc2", MODEL_1_ID, DAY_1, DAY_PERIOD_1, 12, 0L, OLD_VERSION}, // replaced with new version stamp
                {"acc3", MODEL_1_ID, DAY_1, DAY_PERIOD_1, 12, 0L, OLD_VERSION}, // not changed
                {"acc2", MODEL_2_ID, DAY_2, DAY_PERIOD_2, 12, 0L, OLD_VERSION} // not changed
        };
        return uploadHdfsDataUnit(data, fields);
    }

    private String setupBuyingScoreDeltaImport() {
        List<Pair<String, Class<?>>> fields = Arrays.asList(
                Pair.of(AccountId.name(), String.class),
                Pair.of(modelName, String.class),
                Pair.of(Date, Long.class),
                Pair.of(LastActivityDate.name(), Long.class),
                Pair.of(BuyingScore, Double.class)
        );
        Object[][] data = new Object[][]{
                {"acc1", MODEL_1, DAY_1_EPOCH, DAY_1_EPOCH, 0.3}, // acc1 model1 changed from researching to buying
                {"acc1", MODEL_1, DAY_1_EPOCH_LATE, DAY_1_EPOCH_LATE, 0.9}, // acc1 model1 changed from researching to buying
                {"acc2", MODEL_2, DAY_1_EPOCH, DAY_1_EPOCH, 0.1}, // new record for acc2 model 2
                {"acc3", MODEL_2, DAY_2_EPOCH, DAY_2_EPOCH, 0.2} // new record for acc3 model2 day 2
        };
        return uploadHdfsDataUnit(data, fields);
    }

    private String setupBuyingScoreBatchStore() {
        List<Pair<String, Class<?>>> fields = Arrays.asList(
                Pair.of(AccountId.name(), String.class),
                Pair.of(modelNameId, String.class),
                Pair.of(modelName, String.class),
                Pair.of(Date, Long.class),
                Pair.of(__StreamDate.name(), String.class),
                Pair.of(StreamDateId.name(), Integer.class),
                Pair.of(__Row_Count__.name(), Integer.class),
                Pair.of(LastActivityDate.name(), Long.class),
                Pair.of(VERSION_COL, Long.class),
                Pair.of(BuyingScore, Double.class)
        );
        Object[][] data = new Object[][]{
                {"acc1", MODEL_1_ID, MODEL_1, DAY_1_EPOCH, DAY_1, DAY_PERIOD_1, 1, DAY_1_EPOCH, OLD_VERSION, 0.1},
                {"acc2", MODEL_1_ID, MODEL_1, DAY_1_EPOCH, DAY_1, DAY_PERIOD_1, 1, DAY_1_EPOCH, OLD_VERSION, 0.4},
                {"acc999", MODEL_1_ID, MODEL_1, DAY_9_EPOCH, DAY_9, DAY_PERIOD_9, 1, DAY_9_EPOCH, OLD_VERSION, 0.6} // not affected day range
        };
        return uploadHdfsDataUnit(data, fields);
    }

    private String setupIntentActivityRawStreamData() {
        List<Pair<String, Class<?>>> fields = Arrays.asList(
                Pair.of(AccountId.name(), String.class),
                Pair.of(modelName, String.class),
                Pair.of(Date, Long.class),
                Pair.of(__StreamDate.name(), String.class),
                Pair.of(StreamDateId.name(), Integer.class),
                Pair.of(BuyingScore, Double.class)
        );
        Object[][] data = new Object[][]{
                {"acc1", MODEL_1, DAY_1_EPOCH, DAY_1, DAY_PERIOD_1, 0.9},
                {"acc1", MODEL_2, DAY_1_EPOCH, DAY_1, DAY_PERIOD_1, 0.1},
                {"acc1", MODEL_1, DAY_2_EPOCH, DAY_2, DAY_PERIOD_2, 0.1},
                {"acc2", MODEL_1, DAY_2_EPOCH, DAY_2, DAY_PERIOD_2, 0.2},
                {"acc2", MODEL_2, DAY_2_EPOCH, DAY_2, DAY_PERIOD_2, 0.2}
        };
        return uploadHdfsDataUnit(data, fields);
    }

    private String setupRawTransactionStreamData() {
        List<Pair<String, Class<?>>> fields = Arrays.asList(
                Pair.of(TransactionId.name(), String.class), // unused
                Pair.of(AccountId.name(), String.class),
                Pair.of(ContactId.name(), String.class),
                Pair.of(ProductId.name(), String.class),
                Pair.of(TransactionTime.name(), Long.class),
                Pair.of(__StreamDate.name(), String.class),
                Pair.of(StreamDateId.name(), Integer.class),
                Pair.of(Cost.name(), Double.class),
                Pair.of(Quantity.name(), Integer.class),
                Pair.of(Amount.name(), Integer.class),
                Pair.of(TransactionType.name(), String.class),
                Pair.of(ProductType.name(), String.class)
        );
        Object[][] data = new Object[][]{
                {"t1", "a1", "c1", "p1", DAY_1_EPOCH, DAY_1, DAY_PERIOD_1, 0.2, 2, 20, "Purchase", "Spending"},
                {"t2", "a1", "c1", "p1", DAY_1_EPOCH, DAY_1, DAY_PERIOD_1, 0.4, 4, 40, "Purchase", "Spending"},
                {"t3", "a1", "c1", "p1", DAY_2_EPOCH, DAY_2, DAY_PERIOD_2, 0.7, 7, 70, "Purchase", "Spending"},
                {"t4", "a1", "c1", "p2", DAY_1_EPOCH, DAY_1, DAY_PERIOD_1, 0.1, 1, 10, "Purchase", "Spending"}
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

    private String prepareTestData() {
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
        return uploadHdfsDataUnit(data, RAW_STREAM_FIELDS);
    }

    private AggDailyActivityConfig baseConfig() {
        AggDailyActivityConfig config = new AggDailyActivityConfig();
        SparkIOMetadataWrapper inputMetadata = new SparkIOMetadataWrapper();
        Map<String, SparkIOMetadataWrapper.Partition> detailsMap = new HashMap<>();
        SparkIOMetadataWrapper.Partition details = new SparkIOMetadataWrapper.Partition();
        details.setStartIdx(0);
        detailsMap.put(STREAM_ID, details);
        inputMetadata.setMetadata(detailsMap);
        config.inputMetadata = inputMetadata;
        config.streamDateAttrs.put(STREAM_ID, StreamDateId.name());
        config.dimensionMetadataMap.put(STREAM_ID, webVisitMetadata());
        config.dimensionCalculatorMap.put(STREAM_ID, webVisitDimensionCalculators());
        config.hashDimensionMap.put(STREAM_ID, Sets.newHashSet(SourceMediumId.name(), PathPatternId.name()));
        config.additionalDimAttrMap.put(STREAM_ID, Arrays.asList(AccountId.name(), UserId.name()));
        config.dimensionValueIdMap.putAll(WEBVISIT_DIMENSION_HASH_ID_MAP);
        config.currentEpochMilli = DAY_0_EPOCH;
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

    private Map<String, DimensionMetadata> opportunityMetadataWithDerivedDimension() {
        Map<String, DimensionMetadata> metadataMap = new HashMap<>();
        metadataMap.put(StageNameId.name(), stageMetadata());
        metadataMap.put(DerivedId.name(), new DimensionMetadata()); // no need for dimension metadata to calculate
        return metadataMap;
    }

    private Map<String, DimensionMetadata> intentMetadata() {
        Map<String, DimensionMetadata> metadataMap = new HashMap<>();
        metadataMap.put(modelNameId, modelNameMetadata());
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

    private DimensionMetadata modelNameMetadata() {
        DimensionMetadata metadata = new DimensionMetadata();
        metadata.setDimensionValues(
                Arrays.asList(intentModelValue(MODEL_1), intentModelValue(MODEL_2))
        );
        metadata.setCardinality(2);
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

    private Map<String, Object> intentModelValue(String modelName) {
        Map<String, Object> values = new HashMap<>();
        values.put(modelName, modelName);
        values.put(modelNameId, INTENT_DIMENSION_HASH_ID_MAP.get(DimensionGenerator.hashDimensionValue(modelName)));
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

    private Map<String, DimensionCalculator> opportunityDimensionCalculatorsWithDerivedDimension() {
        Map<String, DimensionCalculator> calculatorMap = new HashMap<>();

        DimensionCalculator stageCalculator = new DimensionCalculator();
        stageCalculator.setName(StageName.name());
        stageCalculator.setAttribute(StageName.name());
        calculatorMap.put(StageNameId.name(), stageCalculator);

        CompositeDimensionCalculator derivedCalculator = new CompositeDimensionCalculator();
        derivedCalculator.deriveConfig = getDeriveConfig();
        calculatorMap.put(DerivedId.name(), derivedCalculator);
        return calculatorMap;
    }

    private DeriveConfig getDeriveConfig() {
        DeriveConfig config = new DeriveConfig();
        config.sourceAttrs = Arrays.asList(UserId.name(), StageName.name());
        config.patterns = new ArrayList<>();
        config.patterns.add(Arrays.asList(DERIVED_NAME_1, "bu1", STAGE_WON));
        config.patterns.add(Arrays.asList(DERIVED_NAME_2, "bu2", STAGE_WON));
        return config;
    }

    private Map<String, DimensionCalculator> intentDimensionCalculators() {
        Map<String, DimensionCalculator> calculatorMap = new HashMap<>();

        DimensionCalculator modelNameIdCalculator = new DimensionCalculator();
        modelNameIdCalculator.setName(modelName);
        modelNameIdCalculator.setAttribute(modelName);
        calculatorMap.put(modelNameId, modelNameIdCalculator);

        return calculatorMap;
    }
}
