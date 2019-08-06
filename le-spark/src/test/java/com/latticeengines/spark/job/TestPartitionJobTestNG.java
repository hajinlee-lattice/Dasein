package com.latticeengines.spark.job;

import java.util.Arrays;
import java.util.List;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.TestPartitionJobConfig;
import com.latticeengines.spark.testframework.TestPartitionTestNGBase;

public class TestPartitionJobTestNG extends TestPartitionTestNGBase {


    private List<HdfsDataUnit> inputSources;

    @Test(groups = "functional")
    public void runTest() {
        uploadInputAvro();
        TestPartitionJobConfig config = new TestPartitionJobConfig();
        config.setPartition(true);
        runSparkJob(TestPartitionJob.class, config);
        inputSources = config.getTargets();
    }

    @Test(groups = "functional", dependsOnMethods = "runTest")
    public void runTest2() {
        uploadOutputAsInput(inputSources,"Field2");
        TestPartitionJobConfig config = new TestPartitionJobConfig();
        config.setPartition(false);
        runSparkJob(TestPartitionJob.class, config);
    }
}