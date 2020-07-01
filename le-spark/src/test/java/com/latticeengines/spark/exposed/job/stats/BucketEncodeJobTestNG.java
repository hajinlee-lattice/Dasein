package com.latticeengines.spark.exposed.job.stats;

import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_BOOLEAN_1;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_BOOLEAN_2;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_BOOLEAN_3;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_BOOLEAN_4;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_CAT_MAP_STR;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_CAT_STR;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_ENCODED;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_ENCODED_1;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_ENCODED_2;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_ENCODED_3;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_INTERVAL_DBL;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_INTERVAL_INT;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_NULL_INT;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_RELAY_INT;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_RELAY_STR;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_RENAMED_ROW_ID;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_ROW_ID;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.getBkt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.BitCodecUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.stats.BucketEncodeConfig;
import com.latticeengines.spark.exposed.utils.BucketEncodeUtils;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class BucketEncodeJobTestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        BucketEncodeConfig config = prepareInput();
        SparkJobResult result = runSparkJob(BucketEncodeJob.class, config);
        verifyResult(result);
    }

    private BucketEncodeConfig prepareInput() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(ATTR_ROW_ID, Long.class), //
                Pair.of(ATTR_RELAY_STR, String.class), //
                Pair.of(ATTR_RELAY_INT, Integer.class), //
                Pair.of(ATTR_NULL_INT, Integer.class), //
                Pair.of(ATTR_INTERVAL_INT, Integer.class), //
                Pair.of(ATTR_INTERVAL_DBL, Double.class), //
                Pair.of(ATTR_CAT_STR, String.class), //
                Pair.of(ATTR_CAT_MAP_STR, String.class), //
                Pair.of(ATTR_BOOLEAN_1, String.class), //
                Pair.of(ATTR_BOOLEAN_2, String.class), //
                Pair.of(ATTR_BOOLEAN_3, Integer.class), //
                Pair.of(ATTR_BOOLEAN_4, Boolean.class), //
                Pair.of(ATTR_ENCODED, String.class), //
                Pair.of(ATTR_ENCODED_3, String.class) //
        );
        Object[][] data = new Object[][] { //
                { 1L, "String1", 1, null, 1, 11.0, "Value2", "Group3A", "Y", "0", 1, true, createEncodedString(), "Yes" } };
        uploadHdfsDataUnit(data, fields);

        List<Pair<String, Class<?>>> fields2 = BucketEncodeUtils.profileCols();
        Object[][] data2 = BucketTestUtils.profileData();
        String unitName = uploadHdfsDataUnit(data2, fields2);

        HdfsDataUnit dataUnit = (HdfsDataUnit) getInputUnits().get(unitName);
        String avroGlob = PathUtils.toAvroGlob(dataUnit.getPath());
        List<GenericRecord> profileRecords = AvroUtils.getDataFromGlob(yarnConfiguration, avroGlob);
        BucketEncodeConfig config = new BucketEncodeConfig();
        config.setCodeBookMap(BucketTestUtils.getBitCodeBook());
        config.setCodeBookLookup(BucketTestUtils.getBitCodeLookup());
        config.setEncAttrs(BucketEncodeUtils.encodedAttrs(profileRecords));
        config.setRetainAttrs(BucketEncodeUtils.retainFields(profileRecords));
        config.setRenameFields(BucketEncodeUtils.renameFields(profileRecords));
        return config;
    }

    private String createEncodedString() {
        try {
            return BitCodecUtils.encode(new int[] { 3, 5 });
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate encoded string.", e);
        }
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            System.out.println(record);
            if ((long) record.get(ATTR_RENAMED_ROW_ID) == 1) {
                List<String> tokens = new ArrayList<>();
                tokens.add(ATTR_INTERVAL_INT + "=" + getBkt(record, ATTR_INTERVAL_INT));
                tokens.add(ATTR_INTERVAL_DBL + "=" + getBkt(record, ATTR_INTERVAL_DBL));
                tokens.add(ATTR_CAT_STR + "=" + getBkt(record, ATTR_CAT_STR));
                tokens.add(ATTR_CAT_MAP_STR + "=" + getBkt(record, ATTR_CAT_MAP_STR));
                tokens.add(ATTR_BOOLEAN_1 + "=" + getBkt(record, ATTR_BOOLEAN_1));
                tokens.add(ATTR_BOOLEAN_2 + "=" + getBkt(record, ATTR_BOOLEAN_2));
                tokens.add(ATTR_BOOLEAN_3 + "=" + getBkt(record, ATTR_BOOLEAN_3));
                tokens.add(ATTR_BOOLEAN_4 + "=" + getBkt(record, ATTR_BOOLEAN_4));
                tokens.add(ATTR_ENCODED_1 + "=" + getBkt(record, ATTR_ENCODED_1));
                tokens.add(ATTR_ENCODED_2 + "=" + getBkt(record, ATTR_ENCODED_2));
                tokens.add(ATTR_ENCODED_3 + "=" + getBkt(record, ATTR_ENCODED_3));
                System.out.println(StringUtils.join(tokens, ", "));

                Assert.assertEquals(getBkt(record, ATTR_INTERVAL_INT), 2);
                Assert.assertEquals(getBkt(record, ATTR_INTERVAL_DBL), 3);
                Assert.assertEquals(getBkt(record, ATTR_CAT_STR), 2);
                Assert.assertEquals(getBkt(record, ATTR_CAT_MAP_STR), 3);
                Assert.assertEquals(getBkt(record, ATTR_BOOLEAN_1), 1);
                Assert.assertEquals(getBkt(record, ATTR_BOOLEAN_2), 2);
                Assert.assertEquals(getBkt(record, ATTR_BOOLEAN_3), 1);
                Assert.assertEquals(getBkt(record, ATTR_BOOLEAN_4), 1);
                Assert.assertEquals(getBkt(record, ATTR_ENCODED_1), 1);
                Assert.assertEquals(getBkt(record, ATTR_ENCODED_2), 2);
                Assert.assertEquals(getBkt(record, ATTR_ENCODED_3), 1);
            }
        });
        return true;
    }

}
