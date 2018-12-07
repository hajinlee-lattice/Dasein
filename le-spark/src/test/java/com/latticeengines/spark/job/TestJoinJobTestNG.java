package com.latticeengines.spark.job;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.TestJoinJobConfig;
import com.latticeengines.spark.testframework.TestJoinTestNGBase;

public class TestJoinJobTestNG extends TestJoinTestNGBase {

    @BeforeClass(groups = "functional")
    public void setup() {
        setupLivyEnvironment();
        uploadInputAvro();
    }

    @AfterClass(groups = "functional", alwaysRun = true)
    public void teardown() {
         tearDownLivyEnvironment();
    }

    @Test(groups = "functional")
    public void runTest() {
        TestJoinJobConfig config = new TestJoinJobConfig();
        config.setWorkspace(getWorkspace());
        SparkJobResult result = runSparkJob(TestJoinJob.class, config, null);
        verifyResult(result);
    }

}
