package com.latticeengines.spark.exposed.job.cdl;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.MergeRuleRatingsConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class MergeRuleRatingsTestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        uploadData();
        MergeRuleRatingsConfig config = new MergeRuleRatingsConfig();
        config.setDefaultBucketName("C");
        config.setBucketNames(Arrays.asList("A", "B", "C", "D"));
        SparkJobResult result = runSparkJob(MergeRuleRatings.class, config);
        verifyResult(result);
    }

    private void uploadData() {
        List<Pair<String, Class<?>>> fields1 = Collections.singletonList(Pair.of("AccountId", String.class));
        List<Pair<String, Class<?>>> fields2 = Arrays.asList( //
                Pair.of("AccountId", String.class), //
                Pair.of("DummyAttr", String.class) //
        );

        // default
        Object[][] data = new Object[][] { //
                { "1", "a" }, { "2", "b" }, { "3", "c" }, { "4", "d" }, { "5", "e" }, { "6", null }, //
        };
        uploadHdfsDataUnit(data, fields2);

        // A
        data = new Object[][] { //
                { "1" }, //
        };
        uploadHdfsDataUnit(data, fields1);

        // B
        data = new Object[][] { //
                { "2", null }, { "4", null }, //
        };
        uploadHdfsDataUnit(data, fields2);

        // C
        data = new Object[][] { //
                { "3" }, //
        };
        uploadHdfsDataUnit(data, fields1);

        // D
        data = new Object[][] { //
                { "4" }, { "5" }, //
        };
        uploadHdfsDataUnit(data, fields1);
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            System.out.println(record);
            String accountId = record.get("AccountId").toString();
            String rating = record.get("Rating").toString();
            switch (accountId) {
                case "1":
                    Assert.assertEquals(rating, "A");
                    break;
                case "2":
                    Assert.assertEquals(rating, "B");
                    break;
                case "3":
                    Assert.assertEquals(rating, "C");
                    break;
                case "4":
                    Assert.assertEquals(rating, "B");
                    break;
                case "5":
                    Assert.assertEquals(rating, "D");
                    break;
                default:
                    Assert.assertEquals(rating, "C");
            }
        });
        return true;
    }

}
