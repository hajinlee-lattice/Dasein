package com.latticeengines.spark.job;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.TestPartitionJobConfig;
import com.latticeengines.spark.testframework.TestPartitionTestNGBase;

public class TestPartitionJobTestNG extends TestPartitionTestNGBase {

    @Test(groups = "functional")
    public void runTest() {
        dataCnt = uploadInputAvro();
        TestPartitionJobConfig config = new TestPartitionJobConfig();
        config.setPartition(true);
        SparkJobResult result = runSparkJob(TestPartitionJob.class, config);
        verifier = this::verifyOutput1;
        verifyResult(result);
        inputSources = result.getTargets();
    }

    @Test(groups = "functional", dependsOnMethods = "runTest")
    public void runTest2() {
        copyOutputAsInput(inputSources);
        TestPartitionJobConfig config = new TestPartitionJobConfig();
        config.setPartition(false);
        SparkJobResult result = runSparkJob(TestPartitionJob.class, config);
        verifier = this::verifyOutput2;
        verifyResult(result);
    }
}
