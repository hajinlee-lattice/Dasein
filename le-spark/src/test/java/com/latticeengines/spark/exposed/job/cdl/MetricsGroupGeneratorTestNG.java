package com.latticeengines.spark.exposed.job.cdl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.ActivityRowReducer;
import com.latticeengines.domain.exposed.cdl.activity.ActivityTimeRange;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.transaction.NullMetricsImputation;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ActivityStoreSparkIOMetadata;
import com.latticeengines.domain.exposed.spark.cdl.ActivityStoreSparkIOMetadata.Details;
import com.latticeengines.domain.exposed.spark.cdl.DeriveActivityMetricGroupJobConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;
import com.latticeengines.spark.util.DeriveAttrsUtils;

public class MetricsGroupGeneratorTestNG extends SparkJobFunctionalTestNGBase {

    private static final String TOTAL_VISIT_GROUPNAME = "Total Web Visits";
    private static final String OPPORTUNITY_GROUPNAME = "Opportunity By Stage";
    private static final String MARKETING_GROUPNAME = "Marketing Activity Stream";
    private static final String INTENT_GROUPNAME = "Has Intent";
    private static final String INTENT_TIMERANGE_GROUPNAME = "Intent By Timerange";
    private static final String TOTAL_VISIT_GROUPID = "twv";
    private static final String OPPORTUNITY_GROUPID = "obs";
    private static final String MARKETING_GROUPID = "mas";
    private static final String INTENT_GROUPID = "hix";
    private static final String INTENT_TIMERANGE_GROUPID = "ibt";
    private static final Set<List<Integer>> TIMEFILTER_PARAMS = new HashSet<>(Arrays.asList(
            Collections.singletonList(1),
            Collections.singletonList(2)
    ));
    private static Long WEB_ACTIVITY_ATTR_COUNT;
    private static Long OPPORTUNITY_ATTR_COUNT;
    private static Long MARKETING_ATTR_COUNT;

    private static final Set<String> TIMEFILTER_PERIODS = Collections.singleton(PeriodStrategy.Template.Week.name());
    private static final String EVAL_DATE = "2019-10-24";
    private static final int CUR_PERIODID = 1034;
    private static final int ONE_WEEK_AGO = CUR_PERIODID - 1;
    private static final int TWO_WEEKS_AGO = CUR_PERIODID - 2;
    private static final int FOREVER_AGO = 1;
    private static final long CURRENT_VERSION = 10L;
    private static final long OLD_VERSION = 2L;

    // column names
    private static final String PathPatternId = InterfaceName.PathPatternId.name();
    private static final String AccountId = InterfaceName.AccountId.name();
    private static final String ContactId = InterfaceName.ContactId.name();
    private static final String PeriodId = InterfaceName.PeriodId.name();
    private static final String __Row_Count__ = InterfaceName.__Row_Count__.name();
    private static final String dimNotRequired = "SomeRollupDim";
    private static final String PeriodIdPartition = DeriveAttrsUtils.PARTITION_COL_PREFIX() + PeriodId;
    private static final String OpportunityId = InterfaceName.OpportunityId.name();
    private static final String Stage = InterfaceName.StageName.name();
    private static final String ActivityType = InterfaceName.ActivityType.name();
    private static final String LastActivityDate = InterfaceName.LastActivityDate.name();
    private static final String versionStamp = "versionStamp";

    private static AtlasStream WEB_VISIT_STREAM;
    private static AtlasStream OPPORTUNITY_STREAM;
    private static AtlasStream MARKETING_STREAM;
    private static AtlasStream INTENT_STREAM;
    private static final String WEB_VISIT_STREAM_ID = "webVisit";
    private static final String OPPORTUNITY_STREAM_ID = "opportunity";
    private static final String MARKETING_STREAM_ID = "marketing";
    private static final String INTENT_STREAM_ID = "intent";
    private static final String MODEL_1 = "m1";
    private static final String MODEL_2 = "m2";
    private static final String MODEL_3 = "m3";
    private static final String MODEL_1_ID = "1";
    private static final String MODEL_2_ID = "2";
    private static final String MODEL_3_ID = "3";

    // TODO - followings should move to use interface name
    // TODO - move to column names section
    private static final String modelName = "modelName";
    private static final String modelNameId = "modelNameId";
    private static final String hasIntent = "hasIntent";

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        // setup dummy streams
        WEB_VISIT_STREAM = new AtlasStream();
        WEB_VISIT_STREAM.setStreamId(WEB_VISIT_STREAM_ID);
        WEB_VISIT_STREAM.setAggrEntities(Collections.singletonList(BusinessEntity.Account.name()));
        OPPORTUNITY_STREAM = new AtlasStream();
        OPPORTUNITY_STREAM.setStreamId(OPPORTUNITY_STREAM_ID);
        OPPORTUNITY_STREAM.setAggrEntities(Collections.singletonList(BusinessEntity.Account.name()));
        MARKETING_STREAM = new AtlasStream();
        MARKETING_STREAM.setStreamId(MARKETING_STREAM_ID);
        MARKETING_STREAM.setAggrEntities(Arrays.asList(BusinessEntity.Account.name(), BusinessEntity.Contact.name()));
        INTENT_STREAM = new AtlasStream();
        INTENT_STREAM.setStreamId(INTENT_STREAM_ID);
        INTENT_STREAM.setAggrEntities(Collections.singletonList(BusinessEntity.Account.name()));
    }

    @Test(groups = "functional")
    public void test() {
        String input = appendWebVisitInputData();
        String accountBatchStore = appendAccountBatchStore();
        ActivityMetricsGroup group = setupWebVisitMetricsGroupConfig();
        DeriveActivityMetricGroupJobConfig config = new DeriveActivityMetricGroupJobConfig();
        config.activityMetricsGroups = Collections.singletonList(group);
        config.inputMetadata = constructInputMetadata(WEB_VISIT_STREAM, Collections.singletonList(BusinessEntity.Account));
        config.evaluationDate = EVAL_DATE;
        config.streamMetadataMap = constructWebActivityStreamMetadata();
        config.currentVersionStamp = CURRENT_VERSION;
        WEB_ACTIVITY_ATTR_COUNT = calculateWebActivityAttrsCount(config.streamMetadataMap, group) + 1; // +1 entity Id column
        SparkJobResult result = runSparkJob(MetricsGroupGenerator.class, config, Arrays.asList(input, accountBatchStore), getWorkspace());
        ActivityStoreSparkIOMetadata outputMetadata = JsonUtils.deserialize(result.getOutput(), ActivityStoreSparkIOMetadata.class);
        Assert.assertEquals(outputMetadata.getMetadata().size(), 1);
        verify(result, Collections.singletonList(this::verifyWebVisitMetrics));
    }

    @Test(groups = "functional")
    public void testCountLast() {
        String input = appendOpportunityInput();
        String accountBatchStore = appendAccountBatchStore();
        ActivityMetricsGroup group = setupOpportunityGroupConfig();
        DeriveActivityMetricGroupJobConfig config = new DeriveActivityMetricGroupJobConfig();
        config.activityMetricsGroups = Collections.singletonList(group);
        config.inputMetadata = constructInputMetadata(OPPORTUNITY_STREAM, Collections.singletonList(BusinessEntity.Account));
        config.evaluationDate = EVAL_DATE;
        config.streamMetadataMap = constructOpportunityStreamMetadata();
        config.currentVersionStamp = CURRENT_VERSION;
        OPPORTUNITY_ATTR_COUNT = calculateOpportunityAttrsCount(config.streamMetadataMap, group) + 1;
        SparkJobResult result = runSparkJob(MetricsGroupGenerator.class, config, Arrays.asList(input, accountBatchStore), getWorkspace());
        ActivityStoreSparkIOMetadata outputMetadata = JsonUtils.deserialize(result.getOutput(), ActivityStoreSparkIOMetadata.class);
        Assert.assertEquals(outputMetadata.getMetadata().size(), 1);
        verify(result, Collections.singletonList(this::verifyOpportunityMetrics));
    }

    @Test(groups = "functional")
    public void testContactStream() {
        String input = appendMarketingInput();
        String accountBatchStore = appendAccountBatchStore();
        String contactBatchStore = appendContactBatchStore();
        ActivityMetricsGroup group = setupMarketingGroupConfig();
        DeriveActivityMetricGroupJobConfig config = new DeriveActivityMetricGroupJobConfig();
        config.activityMetricsGroups = Collections.singletonList(group);
        config.inputMetadata = constructInputMetadata(MARKETING_STREAM, Arrays.asList(BusinessEntity.Account, BusinessEntity.Contact));
        config.evaluationDate = EVAL_DATE;
        config.streamMetadataMap = constructMarketingStreamMetadata();
        config.currentVersionStamp = CURRENT_VERSION;
        MARKETING_ATTR_COUNT = calculateMarketingAttrsCount(config.streamMetadataMap, group) + 1;
        SparkJobResult result = runSparkJob(MetricsGroupGenerator.class, config, Arrays.asList(input, accountBatchStore, contactBatchStore), getWorkspace());
        ActivityStoreSparkIOMetadata outputMetadata = JsonUtils.deserialize(result.getOutput(), ActivityStoreSparkIOMetadata.class);
        Assert.assertEquals(outputMetadata.getMetadata().size(), 1);
        verify(result, Collections.singletonList(this::verifyMarketingMetrics));
    }

    @Test(groups = "functional")
    public void testIntentStream() {
        List<String> inputs = Arrays.asList(appendIntentInput(), appendAccountBatchStore());
        DeriveActivityMetricGroupJobConfig config = new DeriveActivityMetricGroupJobConfig();
        config.activityMetricsGroups = Arrays.asList(setupHasIntentGroup(), setupHasIntentByTimeRangeGroup());
        config.inputMetadata = constructInputMetadata(INTENT_STREAM, Collections.singletonList(BusinessEntity.Account));
        config.evaluationDate = EVAL_DATE;
        config.streamMetadataMap = constructIntentMetadata();
        config.currentVersionStamp = CURRENT_VERSION;
        SparkJobResult result = runSparkJob(MetricsGroupGenerator.class, config, inputs, getWorkspace());
        System.out.println(result.getOutput());
        // TODO - automate verification
    }

    private Long calculateWebActivityAttrsCount(Map<String, Map<String, DimensionMetadata>> streamMetadata, ActivityMetricsGroup group) {
        return streamMetadata.get(WEB_VISIT_STREAM_ID).get(PathPatternId).getCardinality()
                * group.getActivityTimeRange().getParamSet().size();
    }

    private Long calculateOpportunityAttrsCount(Map<String, Map<String, DimensionMetadata>> streamMetadata, ActivityMetricsGroup group) {
        return streamMetadata.get(OPPORTUNITY_STREAM_ID).get(Stage).getCardinality();
    }

    private Long calculateMarketingAttrsCount(Map<String, Map<String, DimensionMetadata>> streamMetadata, ActivityMetricsGroup group) {
        return streamMetadata.get(MARKETING_STREAM_ID).get(ActivityType).getCardinality()
                * group.getActivityTimeRange().getParamSet().size();
    }

    private ActivityStoreSparkIOMetadata constructInputMetadata(AtlasStream stream, List<BusinessEntity> batchStoreEntities) {
        ActivityStoreSparkIOMetadata metadata = new ActivityStoreSparkIOMetadata();
        Map<String, Details> detailsMap = new HashMap<>();

        // add period stores details
        Details details = new Details();
        details.setStartIdx(0);
        details.setLabels(new ArrayList<>(TIMEFILTER_PERIODS));
        detailsMap.put(stream.getStreamId(), details);

        // add batch store details
        batchStoreEntities.forEach(entity -> {
            Details batchStoreDetails = new Details();
            batchStoreDetails.setStartIdx(detailsMap.size());
            detailsMap.put(entity.name(), batchStoreDetails);
        });


        metadata.setMetadata(detailsMap);
        return metadata;
    }

    private Map<String, Map<String, DimensionMetadata>> constructWebActivityStreamMetadata() {
        Map<String, Map<String, DimensionMetadata>> metadata = new HashMap<>();
        Map<String, DimensionMetadata> dimensions = new HashMap<>();
        DimensionMetadata pathPatternDimMeta = new DimensionMetadata();
        pathPatternDimMeta.setDimensionValues(Arrays.asList(
                Collections.singletonMap(PathPatternId, "pp1"),
                Collections.singletonMap(PathPatternId, "pp3"),
                Collections.singletonMap(PathPatternId, "pp4"),
                Collections.singletonMap(PathPatternId, "pp5"),
                Collections.singletonMap(PathPatternId, "missingPattern")
        ));
        pathPatternDimMeta.setCardinality(5);
        dimensions.put(PathPatternId, pathPatternDimMeta);
        metadata.put(WEB_VISIT_STREAM_ID, dimensions);
        return metadata;
    }

    private Map<String, Map<String, DimensionMetadata>> constructOpportunityStreamMetadata() {
        Map<String, Map<String, DimensionMetadata>> metadata = new HashMap<>();
        Map<String, DimensionMetadata> dimensions = new HashMap<>();
        DimensionMetadata stageDimMeta = new DimensionMetadata();
        stageDimMeta.setDimensionValues((Arrays.asList(
                Collections.singletonMap(Stage, "won"),
                Collections.singletonMap(Stage, "started"),
                Collections.singletonMap(Stage, "close"),
                Collections.singletonMap(Stage, "lost")
        )));
        stageDimMeta.setCardinality(4);
        dimensions.put(Stage, stageDimMeta);
        metadata.put(OPPORTUNITY_STREAM_ID, dimensions);
        return metadata;
    }

    private Map<String, Map<String, DimensionMetadata>> constructMarketingStreamMetadata() {
        Map<String, Map<String, DimensionMetadata>> metadata = new HashMap<>();
        Map<String, DimensionMetadata> dimensions = new HashMap<>();
        DimensionMetadata marketingMetadata = new DimensionMetadata();
        marketingMetadata.setDimensionValues((Arrays.asList(
                Collections.singletonMap(ActivityType, "SendEmail"),
                Collections.singletonMap(ActivityType, "ClickLink")
        )));
        marketingMetadata.setCardinality(2);
        dimensions.put(ActivityType, marketingMetadata);
        metadata.put(MARKETING_STREAM_ID, dimensions);
        return metadata;
    }

    private Map<String, Map<String, DimensionMetadata>> constructIntentMetadata() {
        Map<String, Map<String, DimensionMetadata>> metadata = new HashMap<>();
        Map<String, DimensionMetadata> dimensions = new HashMap<>();
        DimensionMetadata modelMetadata = new DimensionMetadata();
        modelMetadata.setDimensionValues((Arrays.asList(
                Collections.singletonMap(modelNameId, MODEL_1_ID),
                Collections.singletonMap(modelNameId, MODEL_2_ID),
                Collections.singletonMap(modelNameId, MODEL_3_ID)
        )));
        modelMetadata.setCardinality(3);
        dimensions.put(modelNameId, modelMetadata);
        metadata.put(INTENT_STREAM_ID, dimensions);
        return metadata;
    }

    private String appendWebVisitInputData() {
        List<Pair<String, Class<?>>> periodStoreFields = Arrays.asList( //
                Pair.of(AccountId, String.class), //
                Pair.of(ContactId, String.class), //
                Pair.of(PeriodId, Integer.class), //
                Pair.of(PathPatternId, String.class), //
                Pair.of(__Row_Count__, Integer.class), //
                Pair.of(dimNotRequired, Integer.class), //
                Pair.of(PeriodIdPartition, Integer.class)
        );

        Object[][] data = new Object[][]{ //
                {"acc1", "1", TWO_WEEKS_AGO, null, 5, null, TWO_WEEKS_AGO}, // should be dropped as null in required column
                {"acc1", "1", TWO_WEEKS_AGO, "pp1", 5, null, TWO_WEEKS_AGO}, // should not be dropped as null in required column
                {"acc1", "1", CUR_PERIODID, "pp3", 4, null, CUR_PERIODID}, //
                {"acc1", "1", CUR_PERIODID, "pp5", 3, null, CUR_PERIODID}, //
                {"acc2", "6", TWO_WEEKS_AGO, "pp1", 2, null, TWO_WEEKS_AGO}, //
                {"acc2", "5", TWO_WEEKS_AGO, "pp4", 6, null, TWO_WEEKS_AGO}
        };

        return uploadHdfsDataUnit(data, periodStoreFields);
    }

    private String appendOpportunityInput() {
        List<Pair<String, Class<?>>> periodStoreFields = Arrays.asList(
                Pair.of(AccountId, String.class),
                Pair.of(OpportunityId, String.class),
                Pair.of(PeriodId, Integer.class),
                Pair.of(Stage, String.class),
                Pair.of(__Row_Count__, Integer.class)
        );
        Object[][] data = new Object[][]{
                {"acc1", "opp1", TWO_WEEKS_AGO, "won", 1},
                {"acc2", "opp1", ONE_WEEK_AGO, "close", 1},
                {"acc1", "opp4", TWO_WEEKS_AGO, "close", 1},
                {"acc2", "opp2", TWO_WEEKS_AGO, "started", 1},
                {"acc2", "opp3", ONE_WEEK_AGO, "lost", 1},
                {"acc999", "opp999", FOREVER_AGO, "lost", 1}
        };
        return uploadHdfsDataUnit(data, periodStoreFields);
    }

    private String appendMarketingInput() {
        List<Pair<String, Class<?>>> periodStoreFields = Arrays.asList(
                Pair.of(AccountId, String.class),
                Pair.of(ContactId, String.class),
                Pair.of(PeriodId, Integer.class),
                Pair.of(ActivityType, String.class),
                Pair.of(__Row_Count__, Integer.class)
        );
        Object[][] data = new Object[][]{
                {"acc1", "c1", TWO_WEEKS_AGO, "SendEmail", 10},
                {"acc1", "c2", ONE_WEEK_AGO, "SendEmail", 20}, // contact c2 wrong accountId, use contact batch store as standard
                {"acc2", "c2", ONE_WEEK_AGO, "SendEmail", 30},
                {"invalidAcc", "c4", ONE_WEEK_AGO, "ClickLink", 40}
        };
        return uploadHdfsDataUnit(data, periodStoreFields);
    }

    private String appendIntentInput() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(AccountId, String.class), //
                Pair.of(PeriodId, Integer.class), //
                Pair.of(PeriodIdPartition, Integer.class), //
                Pair.of(__Row_Count__, Integer.class), //
                Pair.of(modelNameId, String.class), //
                Pair.of(hasIntent, Boolean.class), //
                Pair.of(LastActivityDate, Long.class), //
                Pair.of(versionStamp, Long.class)
        );
        Object[][] data = new Object[][]{
                {"acc1", TWO_WEEKS_AGO, TWO_WEEKS_AGO, 22, MODEL_1_ID, true, 0L, OLD_VERSION},
                {"acc1", TWO_WEEKS_AGO, TWO_WEEKS_AGO, 22, MODEL_2_ID, true, 0L, CURRENT_VERSION},
                {"acc2", TWO_WEEKS_AGO, TWO_WEEKS_AGO, 22, MODEL_1_ID, true, 0L, OLD_VERSION},
                {"acc2", ONE_WEEK_AGO, ONE_WEEK_AGO, 22, MODEL_1_ID, true, 0L, OLD_VERSION}
        };
        return uploadHdfsDataUnit(data, fields);
    }

    private String appendAccountBatchStore() {
        List<Pair<String, Class<?>>> accountBatchStoreField = Arrays.asList( //
                Pair.of(AccountId, String.class)
        );

        Object[][] data = new Object[][]{ //
                {"acc1"},
                {"acc2"},
                {"acc999"},
                {"missingAccount"} // add one account missing from activity input data
        };
        return uploadHdfsDataUnit(data, accountBatchStoreField);
    }

    private String appendContactBatchStore() {
        List<Pair<String, Class<?>>> contactBatchStoreField = Arrays.asList( //
                Pair.of(AccountId, String.class),
                Pair.of(ContactId, String.class)
        );

        Object[][] data = new Object[][]{
                {"acc1", "c1"},
                {"acc1", "c2"},
                {"acc2", "c3"},
                {"acc3", "c4"}
        };
        return uploadHdfsDataUnit(data, contactBatchStoreField);
    }

    private ActivityMetricsGroup setupWebVisitMetricsGroupConfig() {
        ActivityMetricsGroup group = new ActivityMetricsGroup();
        group.setStream(WEB_VISIT_STREAM);
        group.setGroupId(TOTAL_VISIT_GROUPID);
        group.setGroupName(TOTAL_VISIT_GROUPNAME);
        group.setJavaClass(Long.class.getSimpleName());
        group.setEntity(BusinessEntity.Account);
        group.setActivityTimeRange(createActivityTimeRange(ComparisonType.WITHIN, TIMEFILTER_PERIODS, TIMEFILTER_PARAMS));
        group.setRollupDimensions(PathPatternId);
        group.setAggregation(createAttributeDeriver(Collections.singletonList(__Row_Count__), __Row_Count__, StreamAttributeDeriver.Calculation.SUM));
        group.setNullImputation(NullMetricsImputation.ZERO);
        return group;
    }

    private ActivityMetricsGroup setupOpportunityGroupConfig() {
        ActivityMetricsGroup group = new ActivityMetricsGroup();
        group.setStream(OPPORTUNITY_STREAM);
        group.setGroupId(OPPORTUNITY_GROUPID);
        group.setGroupName(OPPORTUNITY_GROUPNAME);
        group.setJavaClass(Long.class.getSimpleName());
        group.setEntity(BusinessEntity.Account);
        group.setActivityTimeRange(createActivityTimeRange(ComparisonType.EVER, TIMEFILTER_PERIODS, null));
        group.setRollupDimensions(Stage);
        group.setAggregation(createAttributeDeriver(Collections.singletonList(__Row_Count__), __Row_Count__, StreamAttributeDeriver.Calculation.SUM));
        group.setNullImputation(NullMetricsImputation.ZERO);
        group.setReducer(prepareReducer());
        return group;
    }

    private ActivityMetricsGroup setupMarketingGroupConfig() {
        ActivityMetricsGroup group = new ActivityMetricsGroup();
        group.setStream(MARKETING_STREAM);
        group.setGroupId(MARKETING_GROUPID);
        group.setGroupName(MARKETING_GROUPNAME);
        group.setJavaClass(Long.class.getSimpleName());
        group.setEntity(BusinessEntity.Contact);
        group.setActivityTimeRange(createActivityTimeRange(ComparisonType.WITHIN, TIMEFILTER_PERIODS, new HashSet<>(Collections.singletonList(
                Collections.singletonList(2)
        ))));
        group.setRollupDimensions(ActivityType);
        group.setAggregation(createAttributeDeriver(Collections.singletonList(__Row_Count__), __Row_Count__, StreamAttributeDeriver.Calculation.SUM));
        group.setNullImputation(NullMetricsImputation.ZERO);
        return group;
    }

    private ActivityMetricsGroup setupHasIntentGroup() {
        ActivityMetricsGroup group = new ActivityMetricsGroup();
        group.setStream(INTENT_STREAM);
        group.setGroupId(INTENT_GROUPID);
        group.setGroupName(INTENT_GROUPNAME);
        group.setJavaClass(Boolean.class.getSimpleName());
        group.setEntity(BusinessEntity.Account);
        group.setActivityTimeRange(createActivityTimeRange(ComparisonType.EVER, TIMEFILTER_PERIODS, null));
        group.setRollupDimensions(modelNameId);
        group.setAggregation(createAttributeDeriver(Collections.emptyList(), hasIntent, StreamAttributeDeriver.Calculation.TRUE));
        group.setNullImputation(NullMetricsImputation.FALSE);
        group.setUseLatestVersion(true);
        return group;
    }

    private ActivityMetricsGroup setupHasIntentByTimeRangeGroup() {
        ActivityMetricsGroup group = new ActivityMetricsGroup();
        group.setStream(INTENT_STREAM);
        group.setGroupId(INTENT_TIMERANGE_GROUPID);
        group.setGroupName(INTENT_TIMERANGE_GROUPNAME);
        group.setJavaClass(Boolean.class.getSimpleName());
        group.setEntity(BusinessEntity.Account);
        group.setActivityTimeRange(createActivityTimeRange(ComparisonType.WITHIN, TIMEFILTER_PERIODS, TIMEFILTER_PARAMS));
        group.setRollupDimensions(modelNameId);
        group.setAggregation(createAttributeDeriver(Collections.emptyList(), hasIntent, StreamAttributeDeriver.Calculation.TRUE));
        group.setNullImputation(NullMetricsImputation.FALSE);
        return group;
    }

    private ActivityRowReducer prepareReducer() {
        ActivityRowReducer reducer = new ActivityRowReducer();
        reducer.setGroupByFields(Collections.singletonList(OpportunityId));
        reducer.setArguments(Collections.singletonList(PeriodId));
        reducer.setOperator(ActivityRowReducer.Operator.Latest);
        return reducer;
    }

    private ActivityTimeRange createActivityTimeRange(ComparisonType operator, Set<String> periods,
                                                      Set<List<Integer>> paramSet) {
        ActivityTimeRange timeRange = new ActivityTimeRange();
        timeRange.setOperator(operator);
        timeRange.setPeriods(periods);
        timeRange.setParamSet(paramSet);
        return timeRange;
    }

    private StreamAttributeDeriver createAttributeDeriver(List<String> sourceAttrs, String targetAttr,
                                                          StreamAttributeDeriver.Calculation calculation) {
        StreamAttributeDeriver deriver = new StreamAttributeDeriver();
        deriver.setSourceAttributes(sourceAttrs);
        deriver.setTargetAttribute(targetAttr);
        deriver.setCalculation(calculation);
        return deriver;
    }

    private Boolean verifyWebVisitMetrics(HdfsDataUnit metrics) {
        Object[][] expectedResult = new Object[][]{
                // 4 for each week (1, 2), + 1 entityId
                // w_1_w all zeros
                {"acc1", 0, 0, 0, 0, 0, 5, 0, 0, 0, 0},
                {"acc2", 0, 0, 0, 0, 0, 2, 6, 0, 0, 0},
                {"acc999", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                {"missingAccount", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
        };
        Map<Object, List<Object>> expectedMap = Arrays.stream(expectedResult)
                .collect(Collectors.toMap(arr -> arr[0].toString(), Arrays::asList));
        Iterator<GenericRecord> iterator = verifyAndReadTarget(metrics);
        int rowCount = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iterator) {
            Assert.assertEquals(record.getSchema().getFields().size(), WEB_ACTIVITY_ATTR_COUNT.longValue());
            verifyTargetData(expectedMap, record, AccountId);
            rowCount++;
        }
        Assert.assertEquals(rowCount, expectedResult.length);
        return false;
    }

    private Boolean verifyOpportunityMetrics(HdfsDataUnit metrics) {
        Object[][] expectedResult = new Object[][]{
                // 4 for each stage (close, lost, started, won), + 1 entityId
                {"acc1", 1, 0, 0, 0},
                {"acc2", 1, 1, 1, 0},
                {"acc999", 0, 1, 0, 0},
                {"missingAccount", 0, 0, 0, 0}
        };
        Map<Object, List<Object>> expectedMap = Arrays.stream(expectedResult)
                .collect(Collectors.toMap(arr -> arr[0].toString(), Arrays::asList));
        Iterator<GenericRecord> iterator = verifyAndReadTarget(metrics);
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iterator) {
            Assert.assertEquals(record.getSchema().getFields().size(), OPPORTUNITY_ATTR_COUNT.longValue());
            verifyTargetData(expectedMap, record, AccountId);
        }
        return false;
    }

    private Boolean verifyMarketingMetrics(HdfsDataUnit metrics) {
        Object[][] expectedResult = new Object[][]{
                {"c1", 0, 10},
                {"c2", 0, 50},
                {"c3", 0, 0},
                {"c4", 40, 0}
        };
        Map<Object, List<Object>> expectedMap = Arrays.stream(expectedResult)
                .collect(Collectors.toMap(arr -> arr[0].toString(), Arrays::asList));
        Iterator<GenericRecord> iterator = verifyAndReadTarget(metrics);
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iterator) {
            Assert.assertEquals(record.getSchema().getFields().size(), MARKETING_ATTR_COUNT.longValue());
            verifyTargetData(expectedMap, record, ContactId);
        }
        return false;
    }

    private void verifyTargetData(Map<Object, List<Object>> expectedMap, GenericRecord record, String entityIdCol) {
        Assert.assertNotNull(record);
        String entityId = record.get(entityIdCol).toString();
        Assert.assertNotNull(expectedMap.get(entityId));
        List<Object> actual = record.getSchema().getFields().stream().map(field -> record.get(field.name()).toString()).collect(Collectors.toList());
        Assert.assertEquals(actual, expectedMap.get(entityId).stream().map(Object::toString).collect(Collectors.toList()));
    }
}
