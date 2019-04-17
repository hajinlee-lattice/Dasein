package com.latticeengines.datacloud.dataflow.bucket;

import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.ATTR_RELAY_INT;
import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.ATTR_RELAY_STR;
import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.ATTR_RENAMED_ROW_ID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.dataflow.utils.BucketEncodeUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.CalculateStatsConfig;

public class AdvancedCalcStatsTestNG extends CalcStatsTestNG {

    @Override
    @Test(groups = "functional")
    public void test() throws Exception {
        TransformationFlowParameters parameters = prepareInput();
        executeDataFlow(parameters);
        verifyResult();
    }

    private TransformationFlowParameters prepareInput() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("EAttr1", Long.class), //
                Pair.of("EAttr2", Long.class), //
                Pair.of("EAttr3", Long.class), //
                Pair.of(ATTR_RENAMED_ROW_ID, Long.class), //
                Pair.of(ATTR_RELAY_STR, String.class), //
                Pair.of(ATTR_RELAY_INT, Integer.class), //
                Pair.of("D", Integer.class), //
                Pair.of("A1", String.class), //
                Pair.of("A2", String.class), //
                Pair.of("B", String.class), //
                Pair.of("C", String.class) //
        );
        Object[][] data = new Object[][] { //
                { 0L, 0L, 0L, 1L, null, 0, 1, "1", "2", "1", "1" }, //
                { 0L, 0L, 0L, 2L, null, 200, 2, "1", "2", "1", "1" }, //
                { 0L, 0L, 0L, 3L, null, null, 3, "1", "3", "1", "1" }, //
                { 0L, 0L, 0L, 4L, null, 10, 4, "1", "4", "1", "1"  }, //
                { 0L, 0L, 0L, 5L, "String5", 4, 99, "1", "5", "1", "1"  }, //
                { 0L, 0L, 0L, 1L, null, 0, 5, "1", "2", "1", "1" }, //
                { 0L, 0L, 0L, 2L, null, 200, 6, "1", "2", "1", "1" }, //
                { 0L, 0L, 0L, 3L, null, null, 7, "1", "3", "1", "1" }, //
                { 0L, 0L, 0L, 4L, null, 10, 8, "1", "4", "1", "1"  }, //
                { 0L, 0L, 0L, 5L, "String5", 4, 99, "1", "6", "1", "1"  } //
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

        CalculateStatsConfig conf = new CalculateStatsConfig();
        Map<String, List<String>> dims = new HashMap<>();
        dims.put("A1", Collections.singletonList("A2"));
        dims.put("A2", null);
        dims.put("B", null);
        dims.put("C", null);
        conf.setDimensionTree(dims);
        conf.setDedupFields(Collections.singletonList("D"));

        TransformationFlowParameters parameters = new TransformationFlowParameters();
        parameters.setBaseTables(Arrays.asList(AVRO_INPUT, PROFILE));
        parameters.setConfJson(JsonUtils.serialize(conf));
        return parameters;
    }

    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        List<List<Object>> intervalIntData = new ArrayList<>();
        List<List<Object>> relayStrData = new ArrayList<>();
        for (GenericRecord record : records) {
            if ("IntervalInt".equals(record.get("AttrName").toString())) {
                intervalIntData.add(Arrays.asList(
                        record.get("A1"), //
                        record.get("A2"), //
                        record.get("B"), //
                        record.get("C"), //
                        record.get("BktCounts")));
            }
            if (ATTR_RELAY_STR.equals(record.get("AttrName").toString())) {
                relayStrData.add(Arrays.asList(
                        record.get("A1"), //
                        record.get("A2"), //
                        record.get("B"), //
                        record.get("C"), //
                        record.get("BktCounts")));
            }
        }

        String fmt = "%8s\t%8s\t%8s\t%8s\t%s\n";

        System.out.println("\nData for IntervalInt:\n");
        for (List<Object> row: intervalIntData) {
            System.out.format(fmt, row.toArray(new Object[row.size()]));
        }

        System.out.println("\nData for RelayStr:\n");
        for (List<Object> row: relayStrData) {
            System.out.format(fmt, row.toArray(new Object[row.size()]));
        }

        verifyIntervalInt(intervalIntData);
        verifyRelayStr(relayStrData);
    }

    private void verifyIntervalInt(List<List<Object>> data) {
        for (List<Object> row: data) {
            String A1 = row.get(0).toString();
            String A2 = row.get(1).toString();
            String bktCnts = row.get(4).toString();
            // cannot parent = __ALL__ and child = ??
            if ("__ALL__".equals(A1)) {
                Assert.assertEquals(A2, "__ALL__");
            }
            switch (A2) {
                case "2":
                    Assert.assertEquals(bktCnts, "2:4");
                    break;
                case "3":
                    Assert.assertEquals(bktCnts, "3:2");
                    break;
                case "4":
                    Assert.assertEquals(bktCnts, "4:2");
                    break;
                case "__ALL__":
                    Assert.assertEquals(bktCnts, "2:4|3:2|4:2");
                    break;
            }
        }
    }

    private void verifyRelayStr(List<List<Object>> data) {
        for (List<Object> row: data) {
            String bktCnts = row.get(4).toString();
            Assert.assertEquals(bktCnts, "1:1"); // there is only on dedup id value D=1
        }
    }

}
