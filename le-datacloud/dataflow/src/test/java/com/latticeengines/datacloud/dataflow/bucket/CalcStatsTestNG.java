package com.latticeengines.datacloud.dataflow.bucket;

import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.ATTR_BOOLEAN_4;
import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.ATTR_CAT_STR;
import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.ATTR_ENCODED_1;
import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.ATTR_ENCODED_2;
import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.ATTR_RELAY_INT;
import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.ATTR_RELAY_STR;
import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.ATTR_RENAMED_ROW_ID;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_BKTS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_COUNT;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_NAME;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.datacloud.dataflow.transformation.CalculateStats;
import com.latticeengines.datacloud.dataflow.utils.BucketEncodeUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;

public class CalcStatsTestNG extends DataCloudDataFlowFunctionalTestNGBase {

    private static final int ENC_ATTR_1 = 4;
    private static final int ENC_ATTR_2 = 5;
    private static final int ENC_ATTR_3 = 6;

    private static final String PROFILE = "profile";

    @Override
    protected String getFlowBeanName() {
        return CalculateStats.BEAN_NAME;
    }

    @Test(groups = "functional")
    public void test() throws Exception {
        TransformationFlowParameters parameters = prepareInput();
        executeDataFlow(parameters);
        verifyResult();
    }

    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        for (GenericRecord record : records) {
            System.out.println(record);
            String attrName = record.get(STATS_ATTR_NAME).toString();
            long attrCnt = (long) record.get(STATS_ATTR_COUNT);
            Object attrBktsRaw = record.get(STATS_ATTR_BKTS);
            String attrBkts = attrBktsRaw == null ? null : attrBktsRaw.toString();
            if (Arrays.asList(ATTR_ENCODED_1, ATTR_ENCODED_2).contains(attrName)) {
                Assert.assertEquals(attrCnt, 2L);
                Assert.assertEquals(attrBkts, "1:2");
            }
            if (ATTR_CAT_STR.equals(attrName)) {
                Assert.assertEquals(attrCnt, 3L);
                Assert.assertEquals(attrBkts, "1:1|2:1|3:1");
            }
            if (ATTR_BOOLEAN_4.equals(attrName)) {
                Assert.assertEquals(attrCnt, 0);
                Assert.assertTrue(StringUtils.isBlank(attrBkts));
            }
            Assert.assertTrue(attrCnt >= 0);
            Assert.assertTrue(attrCnt <= 5);
            Assert.assertNotEquals(attrName, "IgnoreField");
        }
    }

    private TransformationFlowParameters prepareInput() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(ATTR_RENAMED_ROW_ID, Long.class), //
                Pair.of(ATTR_RELAY_STR, String.class), //
                Pair.of(ATTR_RELAY_INT, Integer.class), //
                Pair.of("IgnoreField", String.class), //
                Pair.of("EAttr1", Long.class), //
                Pair.of("EAttr2", Long.class), //
                Pair.of("EAttr3", Long.class) //
        );
        Object[][] data = new Object[][] { //
                { 1L, "String1", 1, "hello", 0L, 0L, 0L }, //
                { 2L, "String2", 2, "hello", 0L, 0L, 0L }, //
                { 3L, "String3", null, "hello", 0L, 0L, 0L }, //
                { 4L, null, 4, "hello", 0L, 0L, 0L }, //
                { 5L, "String5", 4, "hello", 0L, 0L, 0L } //
        };

        populateIntervalInt(data);
        populateIntervalDouble(data);
        populateCatString(data);
        populateCatMapString(data);
        populateBooleans(data);
        populateYesBits(data);
        uploadDataToSharedAvroInput(data, fields);

        List<Pair<String, Class<?>>> fields2 = BucketEncodeUtils.profileCols();
        Object[][] data2 = BucketTestUtils.profileData();
        uploadAvro(data2, fields2, PROFILE, "/tmp/profile");

        TransformationFlowParameters parameters = new TransformationFlowParameters();
        parameters.setBaseTables(Arrays.asList(AVRO_INPUT, PROFILE));
        return parameters;
    }

    @Override
    protected Map<String, String> extraSourcePaths() {
        return Collections.singletonMap(PROFILE, "/tmp/profile/" + PROFILE + ".avro");
    }

    private void populateIntervalInt(Object[][] data) {
        updateIntervalInt(data, 0, 0);
        updateIntervalInt(data, 1, 1);
        updateIntervalInt(data, 2, 35);
        updateIntervalInt(data, 3, 100);
        updateIntervalInt(data, 4, null);
    }

    private void updateIntervalInt(Object[][] data, int rowNumber, Integer value) {
        data[rowNumber][ENC_ATTR_1] = BucketTestUtils.setIntervalInt((long) data[rowNumber][ENC_ATTR_1], value);
    }

    private void populateIntervalDouble(Object[][] data) {
        updateIntervalDouble(data, 0, null);
        updateIntervalDouble(data, 1, 2.8);
        updateIntervalDouble(data, 2, 10.0);
        updateIntervalDouble(data, 3, -1.4E9);
        updateIntervalDouble(data, 4, null);
    }

    private void updateIntervalDouble(Object[][] data, int rowNumber, Double value) {
        data[rowNumber][ENC_ATTR_1] = BucketTestUtils.setIntervalDouble((long) data[rowNumber][ENC_ATTR_1], value);
    }

    private void populateCatString(Object[][] data) {
        updateCatString(data, 0, "Value1");
        updateCatString(data, 1, null);
        updateCatString(data, 2, "Value2");
        updateCatString(data, 3, null);
        updateCatString(data, 4, "Value3");
    }

    private void updateCatString(Object[][] data, int rowNumber, String value) {
        data[rowNumber][ENC_ATTR_2] = BucketTestUtils.setCatString((long) data[rowNumber][ENC_ATTR_2], value);
    }

    private void populateCatMapString(Object[][] data) {
        updateCatMapString(data, 0, "Group1A");
        updateCatMapString(data, 1, "Group3B");
        updateCatMapString(data, 2, "Group1B");
        updateCatMapString(data, 3, null);
        updateCatMapString(data, 4, null);
    }

    private void updateCatMapString(Object[][] data, int rowNumber, String value) {
        data[rowNumber][ENC_ATTR_2] = BucketTestUtils.setCatMapString((long) data[rowNumber][ENC_ATTR_2], value);
    }

    private void populateBooleans(Object[][] data) {
        updateBooleans(data, 0, new Boolean[] { true, false, true, null });
        updateBooleans(data, 1, new Boolean[] { true, false, false, null });
        updateBooleans(data, 2, new Boolean[] { true, null, null, null });
        updateBooleans(data, 3, new Boolean[] { true, false, true, null });
        updateBooleans(data, 4, new Boolean[] { null, false, false, null });
    }

    private void updateBooleans(Object[][] data, int rowNumber, Boolean[] booleans) {
        data[rowNumber][ENC_ATTR_2] = BucketTestUtils.setBooleans((long) data[rowNumber][ENC_ATTR_2], booleans);
    }

    private void populateYesBits(Object[][] data) {
        updateYesBits(data, 0, new int[] { 3, 1026 });
        updateYesBits(data, 1, new int[] {});
        updateYesBits(data, 2, new int[] { 3 });
        updateYesBits(data, 3, new int[] { 1026 });
        updateYesBits(data, 4, new int[] { 1, 5 });
    }

    private void updateYesBits(Object[][] data, int rowNumber, int[] trueBits) {
        data[rowNumber][ENC_ATTR_3] = BucketTestUtils.setYesBits((long) data[rowNumber][ENC_ATTR_3], trueBits);
    }

}
