package com.latticeengines.spark.job;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.TestJoinJobConfig;
import com.latticeengines.spark.testframework.TestJoinTestNGBase;

public class TestJoinJobTestNG extends TestJoinTestNGBase {

    @Test(groups = "functional")
    public void runTest() {
        uploadInputAvro();
        TestJoinJobConfig config = new TestJoinJobConfig();
        SparkJobResult result = runSparkJob(TestJoinJob.class, config);
        verifyResult(result);

        config.setSpecialTarget(0, DataUnit.DataFormat.PARQUET);
        result = runSparkJob(TestJoinJob.class, config);
        verifyResult(result);
    }

}
