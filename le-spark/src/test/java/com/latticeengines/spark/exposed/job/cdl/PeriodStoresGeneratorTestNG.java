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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver;
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
    private static final String AccountId = "AccountId";
    private static final String ContactId = "ContactId";
    private static final String Count = "Count";
    private static final String SOME_DIM = "SomeDimension";
    private static final String Date = InterfaceName.__StreamDate.name();
    private static final String PeriodIdForPartition = DeriveAttrsUtils.PARTITION_COL_PREFIX() + "PeriodId";
    // DateId in daily store table is not used while generating period stores

    private static List<Pair<String, Class<?>>> INPUT_FIELDS;
    private static List<String> OUTPUT_FIELDS;
    private static List<String> PERIODS = Arrays.asList(PeriodStrategy.Template.Week.name(), PeriodStrategy.Template.Month.name());
    private static AtlasStream STREAM;
    private static final String STREAM_ID = "abc123";

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        setupData();
        setupStream();
    }

    @Test(groups = "functional")
    public void test() {
        DailyStoreToPeriodStoresJobConfig config = new DailyStoreToPeriodStoresJobConfig();
        config.evaluationDate = "2019-10-28";
        config.streams = Collections.singletonList(STREAM);
        config.inputMetadata = createInputMetadata();
        SparkJobResult result = runSparkJob(PeriodStoresGenerator.class, config);
        Assert.assertNotNull(result.getOutput());
        ActivityStoreSparkIOMetadata metadata = JsonUtils.deserialize(result.getOutput(), ActivityStoreSparkIOMetadata.class);
        Assert.assertNotNull(metadata);
        Assert.assertNotNull(metadata.getMetadata().get(STREAM_ID));
        Assert.assertEquals(metadata.getMetadata().get(STREAM_ID).getLabels(), PERIODS);
        verify(result, Arrays.asList(this::verifyWeekPeriodStore, this::verifyMonthPeriodStore));
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

    private void setupData() {
        INPUT_FIELDS = Arrays.asList( //
                Pair.of(AccountId, String.class), //
                Pair.of(ContactId, String.class), //
                Pair.of(PathPatternId, String.class), //
                Pair.of(Date, String.class), //
                Pair.of(Count, Integer.class), //
                Pair.of(SOME_DIM, Integer.class) //
        );
        Object[][] data = new Object[][]{ //
                {"1", "c1", "pp1", "2019-10-01", 1, 99}, //
                {"1", "c1", "pp1", "2019-10-02", 4, 77}, //
                {"1", "c1", "pp1", "2019-10-03", 3, 88}, //
                {"2", "c2", "pp1", "2019-10-01", 5, 88}, //
                {"2", "c2", "pp1", "2019-10-08", 7, 10}, //
                {"2", "c2", "pp2", "2019-10-08", 6, 20}, //
        };
        uploadHdfsDataUnit(data, INPUT_FIELDS);

        OUTPUT_FIELDS = Arrays.asList(AccountId, PathPatternId, PeriodIdForPartition, Count, SOME_DIM);
    }

    private void setupStream() {
        AtlasStream stream = new AtlasStream();
        stream.setStreamId(STREAM_ID);
        stream.setPeriods(PERIODS);
        stream.setDimensions(prepareDimensions());
        stream.setAggrEntities(Collections.singletonList(BusinessEntity.Account.name()));
        stream.setAttributeDerivers(Arrays.asList(
                createAttributeDeriver(null, Count, StreamAttributeDeriver.Calculation.SUM), //
                createAttributeDeriver(null, SOME_DIM, StreamAttributeDeriver.Calculation.MIN) //
        ));

        STREAM = stream;
    }

    private List<StreamDimension> prepareDimensions() {
        StreamDimension pathPatternIdDim = new StreamDimension();
        pathPatternIdDim.setName(PathPatternId);
        return Collections.singletonList(pathPatternIdDim);
    }

    private StreamAttributeDeriver createAttributeDeriver(List<String> sourceAttrs, String targetAttr,
                                                          StreamAttributeDeriver.Calculation calculation) {
        StreamAttributeDeriver deriver = new StreamAttributeDeriver();
        deriver.setSourceAttributes(sourceAttrs);
        deriver.setTargetAttribute(targetAttr);
        deriver.setCalculation(calculation);
        return deriver;
    }

    private Boolean verifyMonthPeriodStore(HdfsDataUnit df) {
        Object[][] expected = new Object[][]{
                {"2", "pp2", 237, 6, 20}, //
                {"1", "pp1", 237, 8, 77}, //
                {"2", "pp1", 237, 12, 10}
        };
        verifyPeriodStore(expected, df);
        return false;
    }

    private Boolean verifyWeekPeriodStore(HdfsDataUnit df) {
        Object[][] expected = new Object[][]{
                {"2", "pp1", 1031, 5, 88}, //
                {"2", "pp1", 1032, 7, 10}, //
                {"2", "pp2", 1032, 6, 20}, //
                {"1", "pp1", 1031, 8, 77}
        };
        verifyPeriodStore(expected, df);
        return false;
    }

    private void verifyPeriodStore(Object[][] expected, HdfsDataUnit df) {
        Map<Object, List<Object>> expectedMap = Arrays.stream(expected)
                .collect(Collectors.toMap(arr -> arr[0].toString() + arr[1] + arr[2], Arrays::asList)); // accountId + pathPatternId + periodId
        Iterator<GenericRecord> iterator = verifyAndReadTarget(df);
        int rowCount = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iterator) {
            verifyTargetData(expectedMap, record);
            rowCount++;
        }
        Assert.assertEquals(rowCount, expected.length);
    }

    private void verifyTargetData(Map<Object, List<Object>> expectedMap, GenericRecord record) {
        Assert.assertNotNull(record);
        Assert.assertNotNull(record.get(AccountId));
        Assert.assertNotNull(record.get(PathPatternId));
        Assert.assertNotNull(record.get(PeriodIdForPartition));

        String key = record.get(AccountId).toString() + record.get(PathPatternId).toString() + record.get(PeriodIdForPartition).toString();
        Assert.assertNotNull(expectedMap.get(key));
        List<Object> actual = OUTPUT_FIELDS.stream().map(field -> record.get(field).toString()).collect(Collectors.toList());
        Assert.assertEquals(actual, expectedMap.get(key).stream().map(Object::toString).collect(Collectors.toList()));
    }
}
