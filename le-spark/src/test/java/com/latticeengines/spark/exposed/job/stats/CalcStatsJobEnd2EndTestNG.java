package com.latticeengines.spark.exposed.job.stats;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.stats.CalcStatsConfig;
import com.latticeengines.domain.exposed.util.StatsCubeUtils;
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
    @Test(groups = "manual")
    public void test() {
        CalcStatsConfig config = new CalcStatsConfig();
        SparkJobResult result = runSparkJob(CalcStatsJob.class, config);
        verifyResult(result);
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        List<GenericRecord> records = new ArrayList<>();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            String attrName = record.get("AttrName").toString();
            if (attrName.contains("Date")) {
                System.out.println(record);
            }
            records.add(record);
        });
        StatsCube statsCube = StatsCubeUtils.parseAvro(records.listIterator());
        statsCube.getStatistics().forEach((attr, stats) -> {
            if (attr.contains("Date")) {
                System.out.println(attr + ": " + JsonUtils.serialize(stats));
            }
        });
        return true;
    }

}
