package com.latticeengines.spark.exposed.job.stats;

import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketAlgorithm;
import com.latticeengines.domain.exposed.datacloud.dataflow.DiscreteBucket;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.stats.CalcStatsConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class CalcStatsJobOpportunityTestNG extends SparkJobFunctionalTestNGBase {

    @Override
    protected String getJobName() {
        return "calcStats";
    }

    @Override
    protected String getScenarioName() {
        return "opportunity";
    }

    @Override
    protected List<String> getInputOrder() {
        return Arrays.asList("Opportunity", "Profile");
    }

    // need a big avro as the Account input
    @Test(groups = "functional")
    public void test() {
        CalcStatsConfig config = new CalcStatsConfig();
        SparkJobResult result = runSparkJob(CalcStatsJob.class, config);
        verifyResult(result);
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            BucketAlgorithm bktAlgo = JsonUtils.deserialize(record.get("BktAlgo").toString(), BucketAlgorithm.class);
            if (bktAlgo instanceof DiscreteBucket) {
                int numVals = ((DiscreteBucket) bktAlgo).getValues().size();
                String bktCntStr = record.get("BktCounts").toString();
                int numCnts = bktCntStr.split("\\|").length;
                Assert.assertEquals(numVals, numCnts, record.toString());
            }
        });
        return true;
    }

}
