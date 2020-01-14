package com.latticeengines.spark.exposed.job.cdl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.activity.ActivityRowReducer;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;
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

    private static final String PathPatternId = "PathPatternId";
    private static final String AccountId = InterfaceName.AccountId.name();
    private static final String PeriodId = InterfaceName.PeriodId.name();
    private static final String OpportunityId = "opportunityId";
    private static final String Stage = "Stage";
    private static final String Count = InterfaceName.__Row_Count__.name();
    private static final String StreamDate = InterfaceName.__StreamDate.name();
    private static final String DATE_ATTR = InterfaceName.LastModifiedDate.name();
    private static final String PeriodIdForPartition = DeriveAttrsUtils.PARTITION_COL_PREFIX() + PeriodId;
    // DateId in daily store table is not used while generating period stores

    private static List<String> OUTPUT_FIELDS_NO_REDUCER;
    private static List<String> OUTPUT_FIELDS_WITH_REDUCER;
    private static List<String> PERIODS = Arrays.asList(PeriodStrategy.Template.Week.name(), PeriodStrategy.Template.Month.name());
    private static List<String> PERIODS_FOR_REDUCER = Collections.singletonList(PeriodStrategy.Template.Week.name());
    private static AtlasStream STREAM;
    private static AtlasStream STREAM_WITH_REDUCER;
    private static final String STREAM_ID = "abc123";

    @Test(groups = "functional")
    public void test() {
        List<String> inputs = Collections.singletonList(setupNoReducer());
        DailyStoreToPeriodStoresJobConfig config = new DailyStoreToPeriodStoresJobConfig();
        config.evaluationDate = "2019-10-28";
        config.streams = Collections.singletonList(STREAM);
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
        List<String> inputs = Collections.singletonList(setupWithReducer());
        DailyStoreToPeriodStoresJobConfig config = new DailyStoreToPeriodStoresJobConfig();
        config.evaluationDate = "2019-10-28";
        config.streams = Collections.singletonList(STREAM_WITH_REDUCER);
        config.inputMetadata = createInputMetadata();
        SparkJobResult result = runSparkJob(PeriodStoresGenerator.class, config, inputs, getWorkspace());
        ActivityStoreSparkIOMetadata metadata = JsonUtils.deserialize(result.getOutput(), ActivityStoreSparkIOMetadata.class);
        Assert.assertNotNull(metadata);
        verify(result, Collections.singletonList(this::verifyReduced));
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
                Pair.of(StreamDate, String.class), //
                Pair.of(Count, Integer.class) //
        );
        Object[][] data = new Object[][]{ //
                {"1", "pp1", "2019-10-01", 1}, //
                {"1", "pp1", "2019-10-02", 4}, //
                {"1", "pp1", "2019-10-03", 3}, //
                {"2", "pp1", "2019-10-01", 5}, //
                {"2", "pp1", "2019-10-08", 7}, //
                {"2", "pp2", "2019-10-08", 6}, //
        };
        setupStream();
        return uploadHdfsDataUnit(data, inputFields);
    }

    private String setupWithReducer() {
        List<Pair<String, Class<?>>> inputFields = Arrays.asList( //
                Pair.of(AccountId, String.class), //
                Pair.of(OpportunityId, String.class), //
                Pair.of(Stage, String.class), //
                Pair.of(DATE_ATTR, String.class), //
                Pair.of(StreamDate, String.class), //
                Pair.of(Count, Integer.class)
        );
        Object[][] data = new Object[][]{
                {"acc1", "opp1", "open", "Oct 21, 2018 18:37", "2018-10-21", 1},
                {"acc1", "opp1", "dev", "Oct 22, 2018 19:37", "2018-10-22", 1},
                {"acc1", "opp1", "won", "Oct 23, 2018 20:37", "2018-10-23", 1},
                {"acc1", "opp1", "close", "Oct 29, 2018 20:37", "2018-10-29", 1}
        };
        setupStreamWithReducer();
        return uploadHdfsDataUnit(data, inputFields);
    }

    private void setupStream() {
        AtlasStream stream = new AtlasStream();
        stream.setStreamId(STREAM_ID);
        stream.setPeriods(PERIODS);
        stream.setDimensions(Collections.singletonList(prepareDimension(PathPatternId)));
        stream.setAggrEntities(Collections.singletonList(BusinessEntity.Account.name()));

        STREAM = stream;
    }

    private void setupStreamWithReducer() {
        AtlasStream stream = new AtlasStream();
        stream.setStreamId(STREAM_ID);
        stream.setPeriods(PERIODS_FOR_REDUCER);
        stream.setDimensions(Arrays.asList(prepareDimension(OpportunityId), prepareDimension(Stage)));
        stream.setAggrEntities(Collections.singletonList(BusinessEntity.Account.name()));
        stream.setReducer(prepareReducer());

        STREAM_WITH_REDUCER = stream;
    }

    private ActivityRowReducer prepareReducer() {
        ActivityRowReducer reducer = new ActivityRowReducer();
        reducer.setGroupByFields(Arrays.asList(OpportunityId, AccountId));
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
        OUTPUT_FIELDS_NO_REDUCER = Arrays.asList(AccountId, PathPatternId, PeriodIdForPartition, Count);
        Object[][] expected = new Object[][]{
                {"2", "pp2", 237, 6}, //
                {"1", "pp1", 237, 8}, //
                {"2", "pp1", 237, 12}
        };
        verifyPeriodStore(expected, df, false);
        return false;
    }

    private Boolean verifyWeekPeriodStore(HdfsDataUnit df) {
        OUTPUT_FIELDS_NO_REDUCER = Arrays.asList(AccountId, PathPatternId, PeriodIdForPartition, Count);
        Object[][] expected = new Object[][]{
                {"2", "pp1", 1031, 5}, //
                {"2", "pp1", 1032, 7}, //
                {"2", "pp2", 1032, 6}, //
                {"1", "pp1", 1031, 8}
        };
        verifyPeriodStore(expected, df, false);
        return false;
    }

    private Boolean verifyReduced(HdfsDataUnit df) {
        OUTPUT_FIELDS_WITH_REDUCER = Arrays.asList(AccountId, OpportunityId, PeriodIdForPartition, Stage, Count);
        Object[][] expected = new Object[][]{
                {"acc1", "opp1", 982, "won", 1}, //
                {"acc1", "opp1", 983, "close", 1} //
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
        List<Object> actual = fields.stream().map(field -> record.get(field).toString()).collect(Collectors.toList());
        Assert.assertEquals(actual, expectedMap.get(key).stream().map(Object::toString).collect(Collectors.toList()));
    }
}
