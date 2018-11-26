package com.latticeengines.hadoop.bean;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import javax.annotation.Resource;
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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;


@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-hadoop-context.xml" })
public class YarnConfigurationTestNG extends AbstractTestNGSpringContextTests {

    @Inject
    private Configuration yarnConfiguration;

    @Resource(name = "distCpConfiguration")
    private Configuration distCpConfiguration;

    @Inject
    private EMRCacheService emrCacheService;

    @Inject
    private S3Service s3Service;

    @Value("${aws.test.s3.bucket}")
    private String s3Bucket;

    @Value("${aws.default.access.key}")
    private String awsKey;

    @Value("${aws.default.secret.key.encrypted}")
    private String awsSecret;

    @Value("${common.le.stack}")
    private String leStack;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    private String hdfsKmsKey = "yarnconfigtest";

    @BeforeClass(groups = "functional")
    public void setup() throws IOException {
        resetEnvironment();
    }

    @AfterClass(groups = "functional")
    public void teardown() throws IOException {
        resetEnvironment();
    }

    private void resetEnvironment() throws IOException {
        if (!Boolean.TRUE.equals(useEmr) && HdfsUtils.keyExists(yarnConfiguration, hdfsKmsKey)) {
            HdfsUtils.deleteKey(yarnConfiguration, hdfsKmsKey);
        }
        if (HdfsUtils.fileExists(yarnConfiguration, "/tmp/HdfsUtilsTest")) {
            HdfsUtils.rmdir(yarnConfiguration, "/tmp/HdfsUtilsTest");
        }
    }

    @Test(groups = "manual", enabled = false)
    public void testEmrYarnConfiguration() throws IOException {
        Assert.assertEquals(yarnConfiguration.get("fs.defaultFS"), //
                String.format("hdfs://%s", emrCacheService.getMasterIp()));
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

    @Test(groups = "functional", enabled = false)
    public void testS3DistCp() throws Exception {
        if (!Boolean.TRUE.equals(useEmr)) {
            testS3DistCp(true);
        }
        testS3DistCp(false);
    }

    private void testS3DistCp(boolean encrypted) throws Exception {
        String queue = "default";

        if (encrypted) {
            if (!HdfsUtils.keyExists(yarnConfiguration, hdfsKmsKey)) {
                HdfsUtils.createKey(yarnConfiguration, hdfsKmsKey);
            }
            HdfsUtils.mkdir(yarnConfiguration, "/tmp/HdfsUtilsTest");
            HdfsUtils.createEncryptionZone(yarnConfiguration, "/tmp/HdfsUtilsTest", hdfsKmsKey);
        }

        // from hdfs to s3
        String srcDir = "/tmp/HdfsUtilsTest/input";
        List<Pair<String, Class<?>>> columns = ImmutableList.of( //
                Pair.of("Id", Integer.class), //
                Pair.of("Value", String.class)
        );
        Object[][] data = new Object[][]{
                { 1, "1" }, //
                { 2, "2" }, //
                { 3, "3" },
        };
        AvroUtils.uploadAvro(yarnConfiguration, data, columns, "test", srcDir);
        Assert.assertTrue(HdfsUtils.isDirectory(yarnConfiguration, srcDir));

        if (encrypted) {
            // check distcp between hdfs
            String srcDir2 = "/tmp/HdfsUtilsTest/input2";
            HdfsUtils.distcp(distCpConfiguration, srcDir, srcDir2, queue);
        }

        // clean up s3 target dir
        String tgtDir = "/" + leStack + "/HdfsUtilsTest/output";
        if (s3Service.isNonEmptyDirectory(s3Bucket, tgtDir)) {
            s3Service.cleanupPrefix(s3Bucket, tgtDir);
        }
        Assert.assertFalse(s3Service.isNonEmptyDirectory(s3Bucket, tgtDir));
        String s3Uri = "s3a://" + s3Bucket + tgtDir;
        HdfsUtils.distcp(distCpConfiguration, srcDir, s3Uri, queue);

        // assert HDFS to S3 copy
        Assert.assertTrue(s3Service.isNonEmptyDirectory(s3Bucket, tgtDir));
        InputStream is = s3Service.readObjectAsStream(s3Bucket, tgtDir + "/test.avro");
        AvroUtils.readFromInputStream(is).forEach(System.out::println);

        // reverse copy
        HdfsUtils.rmdir(yarnConfiguration, srcDir);
        Assert.assertFalse(HdfsUtils.isDirectory(yarnConfiguration, srcDir));
        HdfsUtils.distcp(distCpConfiguration, s3Uri, srcDir, queue);
        Assert.assertTrue(HdfsUtils.isDirectory(yarnConfiguration, srcDir));
        AvroUtils.iterator(yarnConfiguration, srcDir + "/*.avro").forEachRemaining(System.out::println);
    }

    @Test(groups = "functional", enabled = false)
    public void testS3DistCpKms() throws Exception {
        // from hdfs to s3
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

        String tgtDir = "/" + leStack + "/HdfsUtilsTest/output";
        if (s3Service.isNonEmptyDirectory(s3Bucket, tgtDir)) {
            s3Service.cleanupPrefix(s3Bucket, tgtDir);
        }
        Assert.assertFalse(s3Service.isNonEmptyDirectory(s3Bucket, tgtDir));

        // copy to unencrypted folder first
        String tgtDirUnenctyped = tgtDir + "_unencrypted";
        if (s3Service.isNonEmptyDirectory(s3Bucket, tgtDirUnenctyped)) {
            s3Service.cleanupPrefix(s3Bucket, tgtDirUnenctyped);
        }
        Assert.assertFalse(s3Service.isNonEmptyDirectory(s3Bucket, tgtDirUnenctyped));
        String s3Uri = "s3n://" + s3Bucket + tgtDirUnenctyped;

        // demo overwrite aws key and secret
        Properties properties = new Properties();
        properties.setProperty("mapreduce.job.user.classpath.first", "true");
        properties.setProperty("fs.s3n.awsAccessKeyId", awsKey);
        properties.setProperty("fs.s3n.awsSecretAccessKey", awsSecret);
        Configuration configuration = ConfigurationUtils.createFrom(yarnConfiguration, properties);
        HdfsUtils.distcp(configuration, srcDir, s3Uri, "default");

        // move to encrypted folder with kms key
        String kmsKey = "test/ysong";
        s3Service.changeKeyRecursive(s3Bucket, tgtDirUnenctyped, tgtDir, kmsKey);
        s3Service.cleanupPrefix(s3Bucket, tgtDirUnenctyped);

        // assert HDFS to S3 copy: reading does not need ams key
        Assert.assertTrue(s3Service.isNonEmptyDirectory(s3Bucket, tgtDir));
        Assert.assertFalse(s3Service.isNonEmptyDirectory(s3Bucket, tgtDirUnenctyped));
        InputStream is = s3Service.readObjectAsStream(s3Bucket, tgtDir + "/test.avro");
        AvroUtils.readFromInputStream(is).forEach(System.out::println);

        // reverse copy -- also need unencrypted staging
        HdfsUtils.rmdir(yarnConfiguration, srcDir);
        Assert.assertFalse(HdfsUtils.isDirectory(yarnConfiguration, srcDir));
        // staging to default key
        s3Service.changeKeyRecursive(s3Bucket, tgtDir, tgtDirUnenctyped, "");
        HdfsUtils.distcp(yarnConfiguration, s3Uri, srcDir, "default");
        // delete staging
        s3Service.cleanupPrefix(s3Bucket, tgtDirUnenctyped);
        Assert.assertTrue(HdfsUtils.isDirectory(yarnConfiguration, srcDir));
        AvroUtils.iterator(yarnConfiguration, srcDir + "/*.avro").forEachRemaining(System.out::println);
    }

}
