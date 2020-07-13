package com.latticeengines.spark.exposed.job.stats;

import java.util.Arrays;
import java.util.List;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.stats.BucketEncodeConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class BucketEncodeJobEnd2EndTestNG extends SparkJobFunctionalTestNGBase {

    @Override
    protected String getJobName() {
        return "bucketEncode";
    }

    @Override
    protected String getScenarioName() {
        return "end2end";
    }

    @Override
    protected List<String> getInputOrder() {
        return Arrays.asList("Account", "Profile");
    }

    // need a big avro as the Account input
    @Test(groups = "manual")
    public void test() {
        BucketEncodeConfig config = new BucketEncodeConfig();
        SparkJobResult result = runSparkJob(BucketEncodeJob.class, config);
        System.out.println(result.getOutput());
        // verifyResult(result);
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            System.out.println(record);
        });
        return true;
    }

}
