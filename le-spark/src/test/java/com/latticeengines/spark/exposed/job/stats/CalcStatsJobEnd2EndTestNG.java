package com.latticeengines.spark.exposed.job.stats;

import java.util.Arrays;
import java.util.List;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.stats.CalcStatsConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class CalcStatsJobEnd2EndTestNG extends SparkJobFunctionalTestNGBase {

    @Override
    protected String getJobName() {
        return "calcStats";
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
    @Test(groups = "functional", enabled = false)
    public void test() {
        CalcStatsConfig config = new CalcStatsConfig();
        SparkJobResult result = runSparkJob(CalcStatsJob.class, config);
        verifyResult(result);
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            String attrName = record.get("AttrName").toString();
            if (attrName.contains("Date")) {
                System.out.println(record);
            }
        });
        return true;
    }

}
