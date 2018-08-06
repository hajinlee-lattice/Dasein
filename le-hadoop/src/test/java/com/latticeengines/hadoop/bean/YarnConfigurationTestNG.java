package com.latticeengines.hadoop.bean;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.yarn.configuration.ConfigurationUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.latticeengines.aws.emr.EMRService;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;


@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-hadoop-context.xml" })
public class YarnConfigurationTestNG extends AbstractTestNGSpringContextTests {

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private EMRService emrService;

    @Inject
    private S3Service s3Service;

    @Value("${aws.test.s3.bucket}")
    private String s3Bucket;

    @Value("${aws.default.access.key}")
    protected String awsKey;

    @Value("${aws.default.secret.key.encrypted}")
    protected String awsSecret;

    @Value("${common.le.stack}")
    private String leStack;

    @Test(groups = "manual", enabled = false)
    public void testEmrYarnConfiguration() throws IOException {
        Assert.assertEquals(yarnConfiguration.get("fs.defaultFS"), //
                String.format("hdfs://%s", emrService.getMasterIp()));
        InputStream is = getResourceStream();
        String hdfsPath = "/tmp/yarn-config-test/test.txt";
        HdfsUtils.isDirectory(yarnConfiguration, "/Pods/Default/Services/PropData/Sources/HGDataClean");
        if (HdfsUtils.fileExists(yarnConfiguration, hdfsPath)) {
            HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
        }
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, is, hdfsPath);
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, hdfsPath));
    }

    private InputStream getResourceStream() {
        return Thread.currentThread().getContextClassLoader().getResourceAsStream("test.txt");
    }

    @Test(groups = "functional")
    public void testS3DistCp() throws Exception {
        List<Pair<String, Class<?>>> columns = ImmutableList.of( //
                Pair.of("Id", Integer.class), //
                Pair.of("Value", String.class)
        );
        Object[][] data = new Object[][]{
                { 1, "1" }, //
                { 2, "2" }, //
                { 3, "3" },
        };
        String srcDir = "/tmp/HdfsUtilsTest/input";
        AvroUtils.uploadAvro(yarnConfiguration, data, columns, "test", srcDir);
        Assert.assertTrue(HdfsUtils.isDirectory(yarnConfiguration, srcDir));

        String tgtDir = leStack + "/HdfsUtilsTest/output";
        String s3Uri = "s3n://" + s3Bucket + "/" + tgtDir;
        if (s3Service.isNonEmptyDirectory(s3Bucket, tgtDir)) {
            s3Service.cleanupPrefix(s3Bucket, tgtDir);
        }
        Assert.assertFalse(s3Service.isNonEmptyDirectory(s3Bucket, tgtDir));

        // demo overwrite aws key and secret
        Properties properties = new Properties();
        properties.setProperty("fs.s3n.awsAccessKeyId", awsKey);
        properties.setProperty("fs.s3n.awsSecretAccessKey", awsSecret);
        Configuration configuration = ConfigurationUtils.createFrom(yarnConfiguration, properties);
        HdfsUtils.distcp(configuration, srcDir, s3Uri, "default");
        Assert.assertTrue(HdfsUtils.isDirectory(yarnConfiguration, s3Uri));

        InputStream is = s3Service.readObjectAsStream(s3Bucket, tgtDir + "/test.avro");
        AvroUtils.readFromInputStream(is).forEach(System.out::println);

        HdfsUtils.rmdir(yarnConfiguration, srcDir);
        Assert.assertFalse(HdfsUtils.isDirectory(yarnConfiguration, srcDir));
        HdfsUtils.distcp(yarnConfiguration, s3Uri, srcDir, "default");
        Assert.assertTrue(HdfsUtils.isDirectory(yarnConfiguration, srcDir));
        AvroUtils.iterator(yarnConfiguration, srcDir + "/*.avro").forEachRemaining(System.out::println);
    }

}
