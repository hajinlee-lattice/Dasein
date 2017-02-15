package com.latticeengines.eai.service.impl.s3;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ExportContext;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.eai.HdfsToS3Configuration;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.service.ExportService;

public class S3ExportServiceImplTestNG extends EaiFunctionalTestNGBase {

    public static final CustomerSpace TEST_CUSTOMER = CustomerSpace.parse("S3ExportServiceImplTestNG");

    @Autowired
    private ExportService s3ExportService;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private S3Service s3Service;

    private String sourceFilePath;

    private String s3Prefix;

    @Value("${common.le.stack}")
    private String leStack;

    @Value("${aws.test.s3.bucket}")
    private String s3Bucket;

    @BeforeClass(groups = "aws")
    public void setup() throws Exception {
        InputStream avroStream = ClassLoader
                .getSystemResourceAsStream("com/latticeengines/eai/service/impl/file/file.avro");
        sourceFilePath = "/tmp/S3ExportServieImplTestNG/sourceFiles";
        s3Prefix = "S3ExportServieImplTestNG/" + leStack;
        HdfsUtils.rmdir(yarnConfiguration, sourceFilePath);
        HdfsUtils.mkdir(yarnConfiguration, sourceFilePath);
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, avroStream, sourceFilePath + "/file.avro");
    }

    @AfterClass(groups = "aws")
    public void cleanup() throws IOException {
        HdfsUtils.rmdir(yarnConfiguration, sourceFilePath);
        s3Service.cleanupPrefix(s3Bucket, s3Prefix);
    }

    @Test(groups = "aws")
    public void exportMetadataAndDataAndWriteToHdfs() throws Exception {
        ExportContext ctx = new ExportContext(yarnConfiguration);

        HdfsToS3Configuration s3ExportConfig = new HdfsToS3Configuration();
        s3ExportConfig.setExportFormat(ExportFormat.AVRO);
        s3ExportConfig.setExportDestination(ExportDestination.S3);
        s3ExportConfig.setCustomerSpace(TEST_CUSTOMER);
        s3ExportConfig.setS3Bucket(s3Bucket);
        s3ExportConfig.setS3Prefix(s3Prefix);
        s3ExportConfig.setTargetFilename("file.avro");
        s3ExportConfig.setExportInputPath(sourceFilePath + "/file.avro");

        s3ExportService.exportDataFromHdfs(s3ExportConfig, ctx);

        ApplicationId appId = ctx.getProperty(ExportProperty.APPID, ApplicationId.class);
        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        Assert.assertTrue(s3Service.listObjects(s3Bucket, s3Prefix).size() >= 1);
    }

}
