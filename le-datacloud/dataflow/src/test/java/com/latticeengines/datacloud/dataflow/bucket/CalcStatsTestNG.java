package com.latticeengines.datacloud.dataflow.bucket;

import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.ATTR_BOOLEAN_4;
import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.ATTR_CAT_STR;
import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.ATTR_DATE_1;
import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.ATTR_ENCODED_1;
import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.ATTR_ENCODED_2;
import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.ATTR_NULL_INT;
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

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.datacloud.dataflow.transformation.CalculateStats;
import com.latticeengines.datacloud.dataflow.utils.BucketEncodeUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.CalculateStatsConfig;

public class CalcStatsTestNG extends DataCloudDataFlowFunctionalTestNGBase {
    private static final int ENC_ATTR_1 = 0;
    private static final int ENC_ATTR_2 = 1;
    private static final int ENC_ATTR_3 = 2;

    protected static final String PROFILE = "profile";

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
            if (ATTR_DATE_1.equals(attrName)) {
                Assert.assertEquals(attrCnt, 5);
            } else {
                Assert.assertTrue(attrCnt <= 5);
            }
            Assert.assertNotEquals(attrName, "IgnoreField");
        }
    }

    private TransformationFlowParameters prepareInput() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("EAttr1", Long.class), //
                Pair.of("EAttr2", Long.class), //
                Pair.of("EAttr3", Long.class), //
                Pair.of(ATTR_RENAMED_ROW_ID, Long.class), //
                Pair.of(ATTR_RELAY_STR, String.class), //
                Pair.of(ATTR_RELAY_INT, Integer.class), //
                Pair.of(ATTR_NULL_INT, Integer.class), //
                Pair.of("IgnoreField", String.class),
                Pair.of(ATTR_DATE_1, Long.class)//
        );
        Object[][] data = new Object[][] { //
                { 0L, 0L, 0L, 1L, "String1", 0, null, "hello" , 1539999999000L},    // 10/20/2018 01:46:39 AM GMT (< 7 Days)
                { 0L, 0L, 0L, 2L, "String2", 200, null, "hello", 1539475200000L},   // 10/14/2018 12:00:00 AM GMT (< 7 Days)
                { 0L, 0L, 0L, 3L, "String3", null, null, "hello", 1539475199000L }, // 10/13/2018 11:59:59 PM GMT (< 30 Days)
                { 0L, 0L, 0L, 4L, null, 10, null, "hello", 1532303999000L },        // 07/22/2018 11:59:59 PM GMT (< 180 Days)
                { 0L, 0L, 0L, 5L, "String5", 4, null, "hello", 1524441600000L }     // 04/23/2018 12:00:00 AM GMT (Ever)
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
        parameters.setConfJson(JsonUtils.serialize(new CalculateStatsConfig()));
        return parameters;
    }

    @Override
    protected Map<String, String> extraSourcePaths() {
        return Collections.singletonMap(PROFILE, "/tmp/profile/" + PROFILE + ".avro");
    }

    void populateIntervalInt(Object[][] data) {
        Integer val = null;
        for (int i = 0; i < data.length; i++) {
            switch (i % 5) {
                case 0:
                    val = 0;
                    break;
                case 1:
                    val = 1;
                    break;
                case 2:
                    val = 35;
                    break;
                case 3:
                    val = 100;
                    break;
                case 4:
                    val = null;
            }
            updateIntervalInt(data, i, val);
        }
    }

    private void updateIntervalInt(Object[][] data, int rowNumber, Integer value) {
        data[rowNumber][ENC_ATTR_1] = BucketTestUtils.setIntervalInt((long) data[rowNumber][ENC_ATTR_1], value);
    }

    void populateIntervalDouble(Object[][] data) {
        Double val = null;
        for (int i = 0; i < data.length; i++) {
            switch (i % 5) {
                case 0:
                    val = null;
                    break;
                case 1:
                    val = 2.8;
                    break;
                case 2:
                    val = 10.0;
                    break;
                case 3:
                    val = -1.4E9;
                    break;
                case 4:
                    val = null;
            }
            updateIntervalDouble(data, i, val);
        }
    }

    private void updateIntervalDouble(Object[][] data, int rowNumber, Double value) {
        data[rowNumber][ENC_ATTR_1] = BucketTestUtils.setIntervalDouble((long) data[rowNumber][ENC_ATTR_1], value);
    }

    void populateCatString(Object[][] data) {
        String val = null;
        for (int i = 0; i < data.length; i++) {
            switch (i % 5) {
                case 0:
                    val = "Value1";
                    break;
                case 1:
                    val = null;
                    break;
                case 2:
                    val = "Value2";
                    break;
                case 3:
                    val = null;
                    break;
                case 4:
                    val = "Value3";
            }
            updateCatString(data, i, val);
        }
    }

    private void updateCatString(Object[][] data, int rowNumber, String value) {
        data[rowNumber][ENC_ATTR_2] = BucketTestUtils.setCatString((long) data[rowNumber][ENC_ATTR_2], value);
    }

    void populateCatMapString(Object[][] data) {
        String val = null;
        for (int i = 0; i < data.length; i++) {
            switch (i % 5) {
                case 0:
                    val = "Group1A";
                    break;
                case 1:
                    val = "Group3B";
                    break;
                case 2:
                    val = "Group1B";
                    break;
                case 3:
                    val = null;
                    break;
                case 4:
                    val = null;
            }
            updateCatMapString(data, i, val);
        }
    }

    private void updateCatMapString(Object[][] data, int rowNumber, String value) {
        data[rowNumber][ENC_ATTR_2] = BucketTestUtils.setCatMapString((long) data[rowNumber][ENC_ATTR_2], value);
    }

    void populateBooleans(Object[][] data) {
        Boolean[] val = null;
        for (int i = 0; i < data.length; i++) {
            switch (i % 5) {
                case 0:
                    val = new Boolean[] { true, false, true, null };
                    break;
                case 1:
                    val = new Boolean[] { true, false, false, null };
                    break;
                case 2:
                    val = new Boolean[] { true, null, null, null };
                    break;
                case 3:
                    val = new Boolean[] { true, false, true, null };
                    break;
                case 4:
                    val = new Boolean[] { null, false, false, null };
            }
            updateBooleans(data, i, val);
        }
    }

    private void updateBooleans(Object[][] data, int rowNumber, Boolean[] booleans) {
        data[rowNumber][ENC_ATTR_2] = BucketTestUtils.setBooleans((long) data[rowNumber][ENC_ATTR_2], booleans);
    }

    void populateYesBits(Object[][] data) {
        int[] val = null;
        for (int i = 0; i < data.length; i++) {
            switch (i % 5) {
                case 0:
                    val = new int[] { 3, 1026 };
                    break;
                case 1:
                    val = new int[] {};
                    break;
                case 2:
                    val = new int[] { 3 };
                    break;
                case 3:
                    val = new int[] { 1026 };
                    break;
                case 4:
                    val = new int[] { 1, 5 };
            }
            updateYesBits(data, i, val);
        }
    }

    private void updateYesBits(Object[][] data, int rowNumber, int[] trueBits) {
        data[rowNumber][ENC_ATTR_3] = BucketTestUtils.setYesBits((long) data[rowNumber][ENC_ATTR_3], trueBits);
    }

}
