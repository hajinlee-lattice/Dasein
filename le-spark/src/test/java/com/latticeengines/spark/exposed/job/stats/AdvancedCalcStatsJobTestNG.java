package com.latticeengines.spark.exposed.job.stats;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_BKTS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_COUNT;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_NAME;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketAlgorithm;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategoricalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.DateBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.DiscreteBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.IntervalBucket;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.stats.CalcStatsConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;
import com.latticeengines.spark.utils.BucketEncodeUtils;

public class AdvancedCalcStatsJobTestNG extends SparkJobFunctionalTestNGBase {

    private Function<HdfsDataUnit, Boolean> targetVerifier;

    @Test(groups = "functional")
    public void test() {
        uploadTestData();
        CalcStatsConfig config = new CalcStatsConfig();
        SparkJobResult result = runSparkJob(CalcStatsJob.class, config);
        targetVerifier = this::verifyFirstTarget;
        verifyResult(result);

        Map<String, List<String>> dims = new HashMap<>();
        dims.put("A1", Collections.singletonList("A2"));
        dims.put("A2", null);
        dims.put("B", null);
        dims.put("C", null);
        config.setDimensionTree(dims);
        result = runSparkJob(CalcStatsJob.class, config);
        targetVerifier = this::verifySecondTarget;
        verifyResult(result);

        config.setDedupFields(Collections.singletonList("D"));
        result = runSparkJob(CalcStatsJob.class, config);
        targetVerifier = this::verifyThirdTarget;
        verifyResult(result);
    }

    private void uploadTestData() {
        String attrInt1 = "Integer1";
        String attrInt2 = "Integer2";
        String attrStr1 = "String1";
        String attrStr2 = "String2";
        String attrDate = "Date";
        String attrA1 = "A1";
        String attrA2 = "A2";
        String attrB = "B";
        String attrC = "C";
        String attrD = "D";

        long date1 = 1539999999000L; // 10/20/2018 01:46:39 AM GMT (< 7 Days)
        long date2 = 1539475200000L; // 10/14/2018 12:00:00 AM GMT (< 7 Days)
        long date3 = 1539475199000L; // 10/13/2018 11:59:59 PM GMT (< 30 Days)
        long date4 = 1532303999000L; // 07/22/2018 11:59:59 PM GMT (< 180 Days)
        long date5 = 1524441600000L; // 04/23/2018 12:00:00 AM GMT (Ever)

        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(attrInt1, Integer.class), //
                Pair.of(attrInt2, Integer.class), //
                Pair.of(attrStr1, String.class), //
                Pair.of(attrStr2, String.class), //
                Pair.of(attrDate, Long.class), //
                Pair.of(attrA1, String.class), //
                Pair.of(attrA2, String.class), //
                Pair.of(attrB, String.class), //
                Pair.of(attrC, String.class), //
                Pair.of(attrD, Integer.class) //
        );
        Object[][] data = new Object[][] { //
                { 0, null, "a", null, date1, "1", "2", "1", "1", 1 }, //
                { 0, null, "a", null, date1, "1", "2", "1", "1", 1 }, //
                { 200, null, "b", null, date2, "1", "2", "1", "1", 2 }, //
                { null, null, "c", null, date1, "1", "3", "1", "1", 3 }, //
                { 10, null, "d", null, date4, "1", "4", "1", "1",4  }, //
                { 4, null, "e", null, date5, "1", "5", "1", "1", 99  }, //
                { 0, null, "f", null, date3, "1", "2", "1", "1", 5 }, //
                { 200, null, "g", null, date4, "1", "2", "1", "1", 6 }, //
                { null, null, "h", null, date3, "1", "3", "1", "1", 7 }, //
                { 10, null, "i", null, date4, "1", "4", "1", "1", 8  }, //
                { 4, null, "j", null, date5, "1", "6", "1", "1", 99 } //
        };
        uploadHdfsDataUnit(data, fields);

        List<Pair<String, Class<?>>> fields2 = BucketEncodeUtils.profileCols();
        Object[][] data2 = new Object[][] { //
                bktAttr(attrInt1, intervalBucket()), //
                bktAttr(attrInt2, nullDiscreteBucket()), //
                bktAttr(attrStr1, categoricalBucket()), //
                bktAttr(attrStr2, nullCategoricalBucket()), //
                bktAttr(attrDate, new DateBucket(BucketTestUtils.ATTR_DATE_1_CURTIME)), //
        };
        uploadHdfsDataUnit(data2, fields2);
    }

    private static Object[] bktAttr(String attrName, BucketAlgorithm algo) {
        Object[] data = new Object[7];
        data[0] = attrName;
        data[1] = attrName;
        data[2] = null;
        data[3] = null;
        data[4] = null;
        data[5] = null;
        if (algo != null) {
            data[6] = JsonUtils.serialize(algo);
        } else {
            data[6] = null;
        }
        return data;
    }

    private static IntervalBucket intervalBucket() {
        IntervalBucket intervalBucket = new IntervalBucket();
        intervalBucket.setBoundaries(Arrays.asList(0, 10, 100));
        return intervalBucket;
    }

    private static DiscreteBucket nullDiscreteBucket() {
        DiscreteBucket bkt = new DiscreteBucket();
        bkt.setValues(Collections.emptyList());
        return bkt;
    }

    private static CategoricalBucket nullCategoricalBucket() {
        CategoricalBucket bkt = new CategoricalBucket();
        bkt.setCategories(Collections.emptyList());
        return bkt;
    }

    private static CategoricalBucket categoricalBucket() {
        CategoricalBucket bkt = new CategoricalBucket();
        bkt.setCategories(Arrays.asList("a b c d e f g h i j k".split(" ")));
        return bkt;
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        return targetVerifier.apply(tgt);
    }


    private Boolean verifyFirstTarget(HdfsDataUnit tgt) {
        verifyAndReadTarget(tgt).forEachRemaining(this::verifyNonDedupRecord);
        return true;
    }

    private void verifyNonDedupRecord(GenericRecord record) {
        String attr = record.get(STATS_ATTR_NAME).toString();
        Long notNullCount = (Long) record.get(STATS_ATTR_COUNT);
        Object bktCnts = record.get(STATS_ATTR_BKTS);
        switch (attr) {
            case "Integer1":
                Assert.assertEquals(notNullCount, Long.valueOf(9));
                Assert.assertEquals(bktCnts.toString(), "0:2|2:5|3:2|4:2");
                break;
            case "Integer2":
                Assert.assertEquals(notNullCount, Long.valueOf(0));
                Assert.assertEquals(bktCnts.toString(), "0:11");
                break;
            case "String1":
                Assert.assertEquals(notNullCount, Long.valueOf(11));
                Assert.assertEquals(bktCnts.toString(), "1:2|2:1|3:1|4:1|5:1|6:1|7:1|8:1|9:1|10:1");
                break;
            case "String2":
                Assert.assertEquals(notNullCount, Long.valueOf(0));
                Assert.assertEquals(bktCnts.toString(), "0:11");
                break;
            case "Date":
                Assert.assertEquals(notNullCount, Long.valueOf(11));
                Assert.assertEquals(bktCnts.toString(), "1:4|2:2|4:3|5:2");
                break;
            default:
                Assert.assertNotNull(attr);
        }
    }

    private Boolean verifySecondTarget(HdfsDataUnit tgt) {
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
//            if ("Integer1".equals(record.get("AttrName").toString())) {
//                System.out.println(record);
//            }
            if (isTopLevel(record)) {
                verifyNonDedupRecord(record);
            }
            // if parent is __ALL__, child must be __ALL__
            if ("__ALL__".equals(record.get("A1").toString())) {
                Assert.assertEquals(record.get("A2").toString(), "__ALL__");
            }
        });
        return true;
    }

    private Boolean verifyThirdTarget(HdfsDataUnit tgt) {
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
//            if ("String1".equals(record.get("AttrName").toString())) {
//                System.out.println(record);
//            }
            if (isTopLevel(record)) {
                // System.out.println(record);
                String attr = record.get(STATS_ATTR_NAME).toString();
                Long notNullCount = (Long) record.get(STATS_ATTR_COUNT);
                Object bktCnts = record.get(STATS_ATTR_BKTS);
                switch (attr) {
                    case "Integer1":
                        Assert.assertEquals(notNullCount, Long.valueOf(7));
                        Assert.assertEquals(bktCnts.toString(), "0:2|2:3|3:2|4:2");
                        break;
                    case "Integer2":
                        Assert.assertEquals(notNullCount, Long.valueOf(0));
                        Assert.assertEquals(bktCnts.toString(), "0:9");
                        break;
                    case "String1":
                        Assert.assertEquals(notNullCount, Long.valueOf(10));
                        Assert.assertEquals(bktCnts.toString(), "1:1|2:1|3:1|4:1|5:1|6:1|7:1|8:1|9:1|10:1");
                        break;
                    case "String2":
                        Assert.assertEquals(notNullCount, Long.valueOf(0));
                        Assert.assertEquals(bktCnts.toString(), "0:9");
                        break;
                    case "Date":
                        Assert.assertEquals(notNullCount, Long.valueOf(9));
                        Assert.assertEquals(bktCnts.toString(), "1:3|2:2|4:3|5:1");
                        break;
                    default:
                        Assert.assertNotNull(attr);
                }
            }
            // if parent is __ALL__, child must be __ALL__
            if ("__ALL__".equals(record.get("A1").toString())) {
                Assert.assertEquals(record.get("A2").toString(), "__ALL__");
            }
        });
        return true;
    }

    private boolean isTopLevel(GenericRecord record) {
        return "__ALL__".equals(record.get("A1").toString())
            && "__ALL__".equals(record.get("A2").toString())
            && "__ALL__".equals(record.get("B").toString())
            && "__ALL__".equals(record.get("C").toString());
    }

}
