package com.latticeengines.spark.exposed.job.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.SparkCountRecordsConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class SparkCountRecordsJobTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(SparkCountRecordsJobTestNG.class);
    @Inject
    private Configuration yarnConfiguration;

    @Test(groups = "functional")
    public void testAvro() throws IOException {
        // copy local test avro file onto hdfs
        InputStream is = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("com/latticeengines/common/exposed/util/SparkCountRecordsTest/compressed.avro");
        Assert.assertNotNull(is);

        InputStream is2 = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("com/latticeengines/common/exposed/util/SparkCountRecordsTest/compressed2.avro");
        Assert.assertNotNull(is2);

        String tempDir = "/tmp/SparkCountAvroRecords";
        String avroPath = tempDir + "/compressed.avro";
        String avroPath2 = tempDir + "/compressed2.avro";
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, is, avroPath);
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, is2, avroPath2);
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, avroPath));
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, avroPath2));

        // setup wildcard to count records in Avro files
        ArrayList<String> globs = new ArrayList<>();
        globs.add("hdfs://" + tempDir + "/*.avro");
        SparkCountRecordsConfig config = new SparkCountRecordsConfig();
        config.globs = globs.toArray(new String[globs.size()]);
        Assert.assertNotNull(config.globs);
        SparkJobResult result = runSparkJob(SparkCountRecordsJob.class, config);
        Assert.assertEquals(result.getOutput(), "384");
        // TODO - remove temp dir SparkCountAvroRecords
    }

    @Test(groups = "functional", expectedExceptions = Exception.class)
    public void testParquet() throws IOException {
        /* should fail when file is no parquet */
        String[] globs = {"some/meaningless/directory/file.with.invalid.extension"};
        SparkCountRecordsConfig config = new SparkCountRecordsConfig();
        config.globs = globs;
        Assert.assertNotNull(config.globs);
        SparkJobResult result = runSparkJob(SparkCountRecordsJob.class, config);
        Assert.assertTrue(true);
    }
}
