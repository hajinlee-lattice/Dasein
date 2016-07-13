package com.latticeengines.eai.service.impl.camel;

import java.io.File;
import java.io.InputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.eai.route.HdfsToS3RouteConfiguration;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;

@Component("amazonS3ExportServiceTestNG")
public class HdfsToS3RouteTestNG extends EaiFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(HdfsToS3RouteTestNG.class);

    private static final String HDFS_DIR = "/tmp/hdfs2s3";
    private static final String S3_PREFIX = "hdfs2s3";
    private static final String FILENAME = "camel.avro";

    @Autowired
    private HdfsToS3RouteService routeService;

    @Autowired
    private S3Service s3Service;

    @Value("${aws.test.s3.bucket}")
    private String s3Bucket;

    @BeforeClass(groups = "aws")
    public void setup() throws Exception {
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
    public void testDownloadFromSftp() throws Exception {
        HdfsToS3RouteConfiguration configuration = getRouteConfiguration();
        routeService.downloadToLocal(configuration);
        routeService.upload(configuration);
    }

    private void cleanup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, HDFS_DIR);
        s3Service.cleanupPrefix(s3Bucket, S3_PREFIX);
        FileUtils.deleteQuietly(new File("tmp"));
    }

    HdfsToS3RouteConfiguration getRouteConfiguration() {
        HdfsToS3RouteConfiguration configuration = new HdfsToS3RouteConfiguration();
        configuration.setS3Bucket(s3Bucket);
        configuration.setS3Prefix(S3_PREFIX);
        configuration.setHdfsPath(HDFS_DIR + "/*.avro");
        configuration.setTargetFilename(FILENAME);
        configuration.setSplitSize(10L * 1024 * 1024);
        return configuration;
    }

}
