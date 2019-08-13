package com.latticeengines.spark.exposed.job.common;

import java.io.InputStream;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.CalculateDeltaJobConfig;
import com.latticeengines.spark.exposed.job.cdl.CalculateDeltaJob;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class CalculateDeltaJobTestNG extends SparkJobFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(CalculateDeltaJobTestNG.class);

    @Inject
    private Configuration yarnConfiguration;

    @Test(groups = "functional")
    public void testCalculateDelta() throws Exception {
        // copy local test avro file onto hdfs
        InputStream is = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream(
                        "com/latticeengines/common/exposed/util/SparkCountRecordsTest/newAccountData.avro");
        Assert.assertNotNull(is);

        InputStream is2 = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream(
                        "com/latticeengines/common/exposed/util/SparkCountRecordsTest/previousAccountData.avro");
        Assert.assertNotNull(is2);

        InputStream is3 = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream(
                        "com/latticeengines/common/exposed/util/SparkCountRecordsTest/newContactData.avro");
        Assert.assertNotNull(is3);

        InputStream is4 = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream(
                        "com/latticeengines/common/exposed/util/SparkCountRecordsTest/previousContactData.avro");
        Assert.assertNotNull(is4);

        String tempDir = "/tmp/testCalculateDelta";
        String avroPath = tempDir + "/newAccountData.avro";
        String avroPath2 = tempDir + "/previousAccountData.avro";
        String avroPath3 = tempDir + "/newContactData.avro";
        String avroPath4 = tempDir + "/previousContactData.avro";
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, is, avroPath);
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, is2, avroPath2);
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, is3, avroPath3);
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, is4, avroPath4);
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, avroPath));
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, avroPath2));
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, avroPath3));
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, avroPath4));

        CalculateDeltaJobConfig config = new CalculateDeltaJobConfig();
        config.setCurrentAccountUniverse(HdfsDataUnit.fromPath(avroPath));
        config.setPreviousAccountUniverse(HdfsDataUnit.fromPath(avroPath2));
        config.setCurrentContactUniverse(HdfsDataUnit.fromPath(avroPath3));
        config.setPreviousContactUniverse(HdfsDataUnit.fromPath(avroPath4));

        SparkJobResult result = runSparkJob(CalculateDeltaJob.class, config);

        Assert.assertEquals(result.getTargets().size(), 6);
        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 1);
        Assert.assertEquals(result.getTargets().get(1).getCount().intValue(), 3);
        Assert.assertEquals(result.getTargets().get(2).getCount().intValue(), 1);
        Assert.assertEquals(result.getTargets().get(3).getCount().intValue(), 3);
        Assert.assertEquals(result.getTargets().get(4).getCount().intValue(), 8);
        Assert.assertEquals(result.getTargets().get(5).getCount().intValue(), 10);

        // clean up
        result.getTargets().forEach(x -> {
            try {
                HdfsUtils.rmdir(yarnConfiguration, PathUtils.toDirWithoutTrailingSlash(x.getPath()));
            } catch (Exception e) {
                log.info(e.getMessage());
            }
        });
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/testCalculateDelta");
        HdfsUtils.rmdir(yarnConfiguration, result.getTargets().get(0).getPath().replace("Output1", "checkpoints"));
    }

}
