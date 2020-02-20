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
    private static final String TOTAL_VISIT_GROUPID = "twv";
    private static final String OPPORTUNITY_GROUPID = "obs";
    private static final Set<List<Integer>> TIMEFILTER_PARAMS = new HashSet<>(Arrays.asList(
            Collections.singletonList(1),
            Collections.singletonList(2)
    ));
    private static Long WEB_ACTIVITY_ATTR_COUNT;
    private static Long OPPORTUNITY_ATTR_COUNT;

    private static final Set<String> TIMEFILTER_PERIODS = Collections.singleton(PeriodStrategy.Template.Week.name());
    private static final String EVAL_DATE = "2019-10-24";
    private static final int CUR_PERIODID = 1034;
    private static final int ONE_WEEK_AGO = CUR_PERIODID - 1;
    private static final int TWO_WEEKS_AGO = CUR_PERIODID - 2;

    // column names
    private static final String PathPatternId = InterfaceName.PathPatternId.name();
    private static final String AccountId = InterfaceName.AccountId.name();
    private static final String ContactId = InterfaceName.ContactId.name();
    private static final String PeriodId = InterfaceName.PeriodId.name();
    private static final String __Row_Count__ = InterfaceName.__Row_Count__.name();
    private static final String SomeRollupDim = "SomeRollupDim";
    private static final String PeriodIdPartition = DeriveAttrsUtils.PARTITION_COL_PREFIX() + PeriodId;
    private static final String OpportunityId = "OpportunityId";
    private static final String Stage = "Stage";

    private static final String ENTITY_ID_COL = AccountId;

    private static AtlasStream WEB_VISIT_STREAM;
    private static AtlasStream OPPORTUNITY_STREAM;
    private static final String WEB_VISIT_STREAM_ID = "webVisit";
    private static final String OPPORTUNITY_STREAM_ID = "opportunity";

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        // setup dummy streams
        WEB_VISIT_STREAM = new AtlasStream();
        WEB_VISIT_STREAM.setStreamId(WEB_VISIT_STREAM_ID);
        OPPORTUNITY_STREAM = new AtlasStream();
        OPPORTUNITY_STREAM.setStreamId(OPPORTUNITY_STREAM_ID);
    }

    @Test(groups = "functional")
    public void test() {
        String input = appendWebVisitInputData();
        String accountBatchStore = appendAccountBatchStore();
        ActivityMetricsGroup group = setupWebVisitMetricsGroupConfig();
        DeriveActivityMetricGroupJobConfig config = new DeriveActivityMetricGroupJobConfig();
        config.activityMetricsGroups = Collections.singletonList(group);
        config.inputMetadata = constructInputMetadata(WEB_VISIT_STREAM);
        config.evaluationDate = EVAL_DATE;
        config.streamMetadataMap = constructWebActivityStreamMetadata();
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
        config.inputMetadata = constructInputMetadata(OPPORTUNITY_STREAM);
        config.evaluationDate = EVAL_DATE;
        config.streamMetadataMap = constructOpportunityStreamMetadata();
        OPPORTUNITY_ATTR_COUNT = calculateOpportunityAttrsCount(config.streamMetadataMap, group) + 1;
        SparkJobResult result = runSparkJob(MetricsGroupGenerator.class, config, Arrays.asList(input, accountBatchStore), getWorkspace());
        ActivityStoreSparkIOMetadata outputMetadata = JsonUtils.deserialize(result.getOutput(), ActivityStoreSparkIOMetadata.class);
        Assert.assertEquals(outputMetadata.getMetadata().size(), 1);
        verify(result, Collections.singletonList(this::verifyOpportunityMetrics));
    }

    private Long calculateWebActivityAttrsCount(Map<String, Map<String, DimensionMetadata>> streamMetadata, ActivityMetricsGroup group) {
        return streamMetadata.get(WEB_VISIT_STREAM_ID).get(PathPatternId).getCardinality()
                * streamMetadata.get(WEB_VISIT_STREAM_ID).get(SomeRollupDim).getCardinality()
                * group.getActivityTimeRange().getParamSet().size();
    }

    private Long calculateOpportunityAttrsCount(Map<String, Map<String, DimensionMetadata>> streamMetadata, ActivityMetricsGroup group) {
        return streamMetadata.get(OPPORTUNITY_STREAM_ID).get(Stage).getCardinality()
                * group.getActivityTimeRange().getParamSet().size();
    }

    private ActivityStoreSparkIOMetadata constructInputMetadata(AtlasStream stream) {
        ActivityStoreSparkIOMetadata metadata = new ActivityStoreSparkIOMetadata();
        Map<String, Details> detailsMap = new HashMap<>();

        // add period stores details
        Details details = new Details();
        details.setStartIdx(0);
        details.setLabels(new ArrayList<>(TIMEFILTER_PERIODS));
        detailsMap.put(stream.getStreamId(), details);

        // add account batch store details
        Details accountBatchStoreDetails = new Details();
        accountBatchStoreDetails.setStartIdx(1);
        detailsMap.put("Account", accountBatchStoreDetails);

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
        DimensionMetadata someRollupDimMeta = new DimensionMetadata();
        someRollupDimMeta.setDimensionValues(Arrays.asList(
                Collections.singletonMap(SomeRollupDim, 11),
                Collections.singletonMap(SomeRollupDim, 999)
        ));
        someRollupDimMeta.setCardinality(2);
        dimensions.put(PathPatternId, pathPatternDimMeta);
        dimensions.put(SomeRollupDim, someRollupDimMeta);
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

    private String appendWebVisitInputData() {
        List<Pair<String, Class<?>>> periodStoreFields = Arrays.asList( //
                Pair.of(AccountId, String.class), //
                Pair.of(ContactId, String.class), //
                Pair.of(PeriodId, Integer.class), //
                Pair.of(PathPatternId, String.class), //
                Pair.of(__Row_Count__, Integer.class), //
                Pair.of(SomeRollupDim, Integer.class), //
                Pair.of(PeriodIdPartition, Integer.class)
        );

        Object[][] data = new Object[][]{ //
                {"1", "1", TWO_WEEKS_AGO, null, 5, 11, TWO_WEEKS_AGO}, // should be dropped after runAggregation step
                {"1", "1", TWO_WEEKS_AGO, "pp1", 5, 11, TWO_WEEKS_AGO}, //
                {"1", "1", CUR_PERIODID, "pp3", 4, 11, CUR_PERIODID}, //
                {"1", "1", CUR_PERIODID, "pp5", 3, 11, CUR_PERIODID}, //
                {"2", "6", TWO_WEEKS_AGO, "pp1", 2, 11, TWO_WEEKS_AGO}, //
                {"2", "5", TWO_WEEKS_AGO, "pp4", 6, 11, TWO_WEEKS_AGO}
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
                {"acc2", "opp3", ONE_WEEK_AGO, "lost", 1}
        };
        return uploadHdfsDataUnit(data, periodStoreFields);
    }

    private String appendAccountBatchStore() {
        List<Pair<String, Class<?>>> accountBatchStoreField = Arrays.asList( //
                Pair.of(AccountId, String.class)
        );

        Object[][] data = new Object[][]{ //
                {"missingAccount"} // add one account missing from activity input data
        };
        return uploadHdfsDataUnit(data, accountBatchStoreField);
    }

    private ActivityMetricsGroup setupWebVisitMetricsGroupConfig() {
        ActivityMetricsGroup group = new ActivityMetricsGroup();
        group.setStream(WEB_VISIT_STREAM);
        group.setGroupId(TOTAL_VISIT_GROUPID);
        group.setGroupName(TOTAL_VISIT_GROUPNAME);
        group.setJavaClass(Long.class.getSimpleName());
        group.setEntity(BusinessEntity.Account);
        group.setActivityTimeRange(createActivityTimeRange(ComparisonType.WITHIN, TIMEFILTER_PERIODS, TIMEFILTER_PARAMS));
        group.setRollupDimensions(String.format("%s,%s", PathPatternId, SomeRollupDim));
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
        group.setActivityTimeRange(createActivityTimeRange(ComparisonType.WITHIN, TIMEFILTER_PERIODS, TIMEFILTER_PARAMS));
        group.setRollupDimensions(Stage);
        group.setAggregation(createAttributeDeriver(Collections.singletonList(__Row_Count__), __Row_Count__, StreamAttributeDeriver.Calculation.SUM));
        group.setNullImputation(NullMetricsImputation.ZERO);
        group.setReducer(prepareReducer());
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
                // 10 for each week (1, 2), + 1 entityId
                // w_1_w all zeros
                {"1", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                {"2", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 6, 0, 0, 0, 0, 0, 0, 0, 0},
                {"missingAccount", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
        };
        Map<Object, List<Object>> expectedMap = Arrays.stream(expectedResult)
                .collect(Collectors.toMap(arr -> arr[0].toString(), Arrays::asList));
        Iterator<GenericRecord> iterator = verifyAndReadTarget(metrics);
        int rowCount = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iterator) {
            Assert.assertEquals(record.getSchema().getFields().size(), WEB_ACTIVITY_ATTR_COUNT.longValue());
            verifyTargetData(expectedMap, record);
            rowCount++;
        }
        Assert.assertEquals(rowCount, expectedResult.length);
        return false;
    }

    private Boolean verifyOpportunityMetrics(HdfsDataUnit metrics) {
        Object[][] expectedResult = new Object[][]{
                // 4 for each week range (1, 2), + 1 entityId
                {"acc1", 0, 0, 0, 0, 1, 0, 0, 0},
                {"acc2", 1, 1, 0, 0, 1, 1, 1, 0},
                {"missingAccount", 0, 0, 0, 0, 0, 0, 0, 0}
        };
        Map<Object, List<Object>> expectedMap = Arrays.stream(expectedResult)
                .collect(Collectors.toMap(arr -> arr[0].toString(), Arrays::asList));
        Iterator<GenericRecord> iterator = verifyAndReadTarget(metrics);
        int rowCount = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iterator) {
            Assert.assertEquals(record.getSchema().getFields().size(), OPPORTUNITY_ATTR_COUNT.longValue());
            verifyTargetData(expectedMap, record);
        }
        return false;
    }

    private void verifyTargetData(Map<Object, List<Object>> expectedMap, GenericRecord record) {
        Assert.assertNotNull(record);
        String entityId = record.get(ENTITY_ID_COL).toString();
        Assert.assertNotNull(expectedMap.get(entityId));
        List<Object> actual = record.getSchema().getFields().stream().map(field -> record.get(field.name()).toString()).collect(Collectors.toList());
        Assert.assertEquals(actual, expectedMap.get(entityId).stream().map(Object::toString).collect(Collectors.toList()));
    }
}
