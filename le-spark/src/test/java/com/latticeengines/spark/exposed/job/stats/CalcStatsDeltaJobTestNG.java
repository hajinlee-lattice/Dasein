package com.latticeengines.spark.exposed.job.stats;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_BKTS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_COUNT;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_NAME;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.ChangeListConstants;
import com.latticeengines.domain.exposed.spark.stats.CalcStatsDeltaConfig;
import com.latticeengines.domain.exposed.util.StatsCubeUtils;
import com.latticeengines.spark.exposed.utils.BucketEncodeUtils;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class CalcStatsDeltaJobTestNG extends SparkJobFunctionalTestNGBase {

    private Function<HdfsDataUnit, Boolean> targetVerifier;

    @Test(groups = "functional")
    public void test() {
        uploadData();
        CalcStatsDeltaConfig config = new CalcStatsDeltaConfig();
        SparkJobResult result = runSparkJob(CalcStatsDeltaJob.class, config);
        targetVerifier = tgt -> verifyTarget(tgt, null);
        verifyResult(result);

        config.setIncludeAttrs(Arrays.asList("Int1", "Str2", "Bool2", "Int2", "Double2"));
        result = runSparkJob(CalcStatsDeltaJob.class, config);
        targetVerifier = tgt -> verifyTarget(tgt, null);
        verifyResult(result);
    }

    private void uploadData() {
        List<Pair<String, Class<?>>> schema = ChangeListConstants.schema();
        // consider existing column: Str1, Bool1, Int1, Long1, Float1, Double1, Remove
        // new columns: Str2, Bool2, Int2, Double2
        Object[][] data = new Object[][] { //
                // change from null to not-null
                { "null2notnull", "Str1", "String", null, null, "str1", null, null, null, null, null, null,
                        null, null, null, null },
                { "null2notnull", "Bool1", "Boolean", null, null, null, null, true, null, null, null, null,
                        null, null, null, null },
                { "null2notnull", "Int1", "Integer", null, null, null, null, true, null, 1, null, null,
                        null, null, null, null },
                // change from not-null to null
                { "notnull2null", "Str1", "String", null, "str2", null, null, null, null, null, null, null,
                        null, null, null, null },
                { "notnull2null", "Bool1", "Boolean", null, null, null, false, null, null, null, null, null,
                        null, null, null, null },
                { "notnull2null", "Double1", "Double", null, null, null, null, true, null, null, null, null,
                        null, null, 1000.D, null },
                // remove row
                { "removerow", "Str1", "String", true, "str2", null, null, null, null, null, null, null,
                        null, null, null, null },
                { "removerow", "Bool1", "Boolean", true, null, null, false, null, null, null, null, null,
                        null, null, null, null },
                { "removerow", "Int1", "Integer", true, null, null, null, null, 2, null, null, null,
                        null, null, null, null },
                { "removerow", "Long1", "Long", true, null, null, null, null, null, null, 2000L, null,
                        null, null, null, null },
                { "removerow", "Float1", "Float", true, null, null, null, null, null, null, null, null,
                        2.F, null, null, null },
                { "removerow", "Double1", "Double", true, null, null, null, null, null, null, null, null,
                        null, null, 2000.D, null },
                { "removerow", null, null, true, null, null, null, null, null, null, null, null,
                        null, null, null, null },
                // new row
                { "newrow", "Str1", "String", null, null, "str1", null, null, null, null, null, null,
                        null, null, null, null },
                { "newrow", "Bool1", "Boolean", null, null, null, null, true, null, null, null, null,
                        null, null, null, null },
                { "newrow", "Int1", "Integer", null, null, null, null, null, null, 1, null, null,
                        null, null, null, null },
                { "newrow", "Long1", "Long", null, null, null, null, null, null, null, null, 1000L,
                        null, null, null, null },
                { "newrow", "Float1", "Float", null, null, null, null, null, null, null, null, null,
                        null, 1.F, null, null },
                { "newrow", "Double1", "Double", null, null, null, null, null, null, null, null, null,
                        null, null, null, 1000.D },
                { "newrow", null, null, null, null, null, null, null, null, null, null, null,
                        null, null, null, null },
                // new columns
                { "newcol", "Str2", "String", null, null, "str3", null, null, null, null, null, null,
                        null, null, null, null },
                { null, "Str2", "String", null, null, null, null, null, null, null, null, null,
                        null, null, null, null },
                { "newcol", "Bool2", "Boolean", null, null, null, null, true, null, null, null, null,
                        null, null, null, null },
                { null, "Bool2", "Boolean", null, null, null, null, null, null, null, null, null,
                        null, null, null, null },
                { "newcol", "Int2", "Integer", null, null, null, null, null, null, 3, null, null,
                        null, null, null, null },
                { null, "Int2", "Integer", null, null, null, null, null, null, null, null, null,
                        null, null, null, null },
                { "newcol", "Double2", "Double", null, null, null, null, null, null, null, null, null,
                        null, null, null, 3000.D },
                { null, "Double2", "Double", null, null, null, null, null, null, null, null, null,
                        null, null, null, null },
                // remove column
                { "null2notnull", "RemoveStr", "String", true, "str4", null, null, null, null, null, null, null,
                        null, null, null, null },
                { "notnull2null", "RemoveStr", "String", true, "str4", null, null, null, null, null, null, null,
                        null, null, null, null },
                { "removerow", "RemoveStr", "String", true, "str4", null, null, null, null, null, null, null,
                        null, null, null, null },
                { null, "RemoveStr", null, true, null, null, null, null, null, null, null, null,
                        null, null, null, null },
                { "null2notnull", "RemoveInt", "Integer", true, null, null, null, null, 4, null, null, null,
                        null, null, null, null },
                { "notnull2null", "RemoveInt", "Integer", true, null, null, null, null, 4, null, null, null,
                        null, null, null, null },
                { "removerow", "RemoveInt", "Integer", true, null, null, null, null, 4, null, null, null,
                        null, null, null, null },
                { null, "RemoveInt", null, true, null, null, null, null, null, null, null, null,
                        null, null, null, null },
        };
        uploadHdfsDataUnit(data, schema);

        List<Pair<String, Class<?>>> fields2 = BucketEncodeUtils.profileCols();
        Object[][] data2 = new Object[][]{
                BucketTestUtils.categoricalAttr("Str1", Arrays.asList("str1", "str2", "str3", "str4", "str5")),
                BucketTestUtils.booleanAttr("Bool1"),
                BucketTestUtils.discreteAttr("Int1", Arrays.asList(1, 2, 3, 4, 5)),
                BucketTestUtils.discreteAttr("Long1", Arrays.asList(1000L, 2000L, 3000L, 4000L, 5000L)),
                BucketTestUtils.intervalAttr("Float1", Collections.singletonList(1.F)),
                BucketTestUtils.intervalAttr("Double1", Collections.singletonList(1000.F)),
                BucketTestUtils.relayAttr("Str2", "Str2"),
                BucketTestUtils.booleanAttr("Bool2"),
                BucketTestUtils.discreteAttr("Int2", Arrays.asList(1, 2, 3, 4, 5)),
                BucketTestUtils.intervalAttr("Double2", Collections.singletonList(1000.F)),
        };
        uploadHdfsDataUnit(data2, fields2);
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        return targetVerifier.apply(tgt);
    }

    private Boolean verifyTarget(HdfsDataUnit tgt, Collection<String> includeAttrs) {
        List<GenericRecord> records = new ArrayList<>();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            records.add(record);
            String attrName = record.get(STATS_ATTR_NAME).toString();
            long cnt = (long) record.get(STATS_ATTR_COUNT);
            String bktCnt = record.get(STATS_ATTR_BKTS).toString();
            if (includeAttrs == null || includeAttrs.contains(attrName)) {
                switch (attrName) {
                    case "Str1":
                    case "Bool1":
                        Assert.assertEquals(cnt, 0, record.toString());
                        Assert.assertEquals(bktCnt, "1:2|2:-2", record.toString());
                        break;
                    case "Int1":
                        Assert.assertEquals(cnt, 1, record.toString());
                        Assert.assertEquals(bktCnt, "1:2|2:-1", record.toString());
                        break;
                    case "Long1":
                        Assert.assertEquals(cnt, 0, record.toString());
                        Assert.assertEquals(bktCnt, "1:1|2:-1", record.toString());
                        break;
                    case "Float1":
                        Assert.assertEquals(cnt, 0, record.toString());
                        Assert.assertEquals(bktCnt, "2:0", record.toString());
                        break;
                    case "Double1":
                        Assert.assertEquals(cnt, -1, record.toString());
                        Assert.assertEquals(bktCnt, "2:-1", record.toString());
                        break;
                    case "Str2":
                    case "Bool2":
                        Assert.assertEquals(cnt, 1, record.toString());
                        Assert.assertEquals(bktCnt, "1:1", record.toString());
                        break;
                    case "Int2":
                        Assert.assertEquals(cnt, 1, record.toString());
                        Assert.assertEquals(bktCnt, "3:1", record.toString());
                        break;
                    case "Double2":
                        Assert.assertEquals(cnt, 1, record.toString());
                        Assert.assertEquals(bktCnt, "2:1", record.toString());
                        break;
                    default:
                        Assert.fail("Should not see attribute " + attrName + ": " + record.toString());
                }
            } else {
                Assert.fail("Should not see attribute " + attrName + ": " + record.toString());
            }
        });
        StatsCube statsCube = StatsCubeUtils.parseAvro(records.listIterator());
        statsCube.getStatistics().forEach((attr, stats) -> //
                System.out.println(attr + ": " + JsonUtils.serialize(stats)));
        return true;
    }

}
