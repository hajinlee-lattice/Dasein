package com.latticeengines.eai.service.impl.camel;

import java.io.File;
import java.io.InputStream;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.eai.HdfsToS3Configuration;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.service.impl.s3.HdfsToS3ExportService;

@Component("hdfsToS3RouteTestNG")
public class HdfsToS3RouteTestNG extends EaiFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(HdfsToS3RouteTestNG.class);

    private static final String HDFS_DIR = "/tmp/hdfs2s3";
    private static final String S3_PREFIX = "hdfs2s3";
    private static final String FILENAME = "camel.avro";

    @Autowired
    private HdfsToS3ExportService routeService;

    @Autowired
    private S3Service s3Service;

    @Value("${common.le.stack}")
    private String leStack;

    @Value("${aws.test.s3.bucket}")
    private String s3Bucket;

    private String s3Prefix;

    @BeforeClass(groups = "aws")
    public void setup() throws Exception {
        s3Prefix = S3_PREFIX + "/" + leStack;
        cleanup();
        InputStream avroStream = ClassLoader
                .getSystemResourceAsStream("com/latticeengines/eai/service/impl/camel/camel.avro");
        log.info("Uploading test avro to hdfs.");
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, avroStream, HDFS_DIR + "/" + FILENAME);
    }

    @AfterClass(groups = "aws")
    public void teardown() throws Exception {
        cleanup();
    }

    @Test(groups = "aws")
    public void testUploadToS3() throws Exception {
        HdfsToS3Configuration configuration = getRouteConfiguration();
        routeService.downloadToLocal(configuration);
        routeService.upload(configuration);
        Assert.assertTrue(s3Service.listObjects(s3Bucket, s3Prefix).size() > 0);
    }

    private void cleanup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, HDFS_DIR);
        s3Service.cleanupPrefix(s3Bucket, s3Prefix);
        FileUtils.deleteQuietly(new File("tmp"));
    }

    HdfsToS3Configuration getRouteConfiguration() {
        HdfsToS3Configuration configuration = new HdfsToS3Configuration();
        configuration.setS3Bucket(s3Bucket);
        configuration.setS3Prefix(s3Prefix);
        configuration.setExportInputPath(HDFS_DIR + "/*.avro");
        configuration.setTargetFilename(FILENAME);
        configuration.setSplitSize(10L * 1024 * 1024);
        return configuration;
    }

}
