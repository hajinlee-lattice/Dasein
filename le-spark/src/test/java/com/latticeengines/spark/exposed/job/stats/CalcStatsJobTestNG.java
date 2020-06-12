package com.latticeengines.spark.exposed.job.stats;

import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_BOOLEAN_1;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_BOOLEAN_2;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_BOOLEAN_3;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_BOOLEAN_4;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_CAT_STR;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_INTERVAL_DBL;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_INTERVAL_INT;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_NULL_INT;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_RELAY_INT;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_RELAY_STR;
import static com.latticeengines.spark.exposed.job.stats.BucketTestUtils.ATTR_ROW_ID;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.stats.CalcStatsConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;
import com.latticeengines.spark.utils.BucketEncodeUtils;

public class CalcStatsJobTestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        CalcStatsConfig config = prepareInput();
        SparkJobResult result = runSparkJob(CalcStatsJob.class, config);
        verifyResult(result);
    }

    private CalcStatsConfig prepareInput() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(ATTR_ROW_ID, Long.class), //
                Pair.of(ATTR_RELAY_STR, String.class), //
                Pair.of(ATTR_RELAY_INT, Integer.class), //
                Pair.of(ATTR_NULL_INT, Integer.class), //
                Pair.of(ATTR_INTERVAL_INT, Integer.class), //
                Pair.of(ATTR_INTERVAL_DBL, Double.class), //
                Pair.of(ATTR_CAT_STR, String.class), //
                Pair.of(ATTR_BOOLEAN_1, String.class), //
                Pair.of(ATTR_BOOLEAN_2, String.class), //
                Pair.of(ATTR_BOOLEAN_3, Integer.class), //
                Pair.of(ATTR_BOOLEAN_4, Boolean.class)
        );
        Object[][] data = new Object[][] { //
                { 1L, "String1", 1, null, 1, 11.0, "Value2", "Y", "0", 1, true } };
        uploadHdfsDataUnit(data, fields);

        List<Pair<String, Class<?>>> fields2 = BucketEncodeUtils.profileCols();
        Object[][] data2 = BucketTestUtils.profileData();
        uploadHdfsDataUnit(data2, fields2);

        return new CalcStatsConfig();
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            System.out.println(record);
        });
        return true;
    }

}
