package com.latticeengines.eai.service.impl.s3;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportContext;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.service.ExportService;

import edu.emory.mathcs.backport.java.util.Collections;

public class S3ExportServiceImplTestNG extends EaiFunctionalTestNGBase {

    public static final CustomerSpace TEST_CUSTOMER = CustomerSpace.parse("S3ExportServiceImplTestNG");

    @Autowired
    private ExportService s3ExportService;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private S3Service s3Service;

    private String sourceFilePath;

    private String targetS3Path;

    private URL csvUrl;

    @Value("${aws.test.s3.bucket}")
    private String s3Bucket;

    @BeforeClass(groups = "aws")
    public void setup() throws Exception {
        InputStream avroStream = ClassLoader.getSystemResourceAsStream("com/latticeengines/eai/service/impl/file/file.avro");
        sourceFilePath = "/tmp/S3ExportServieImplTestNG/sourceFiles";
        targetS3Path = "S3ExportServieImplTestNG";
        HdfsUtils.rmdir(yarnConfiguration, sourceFilePath);
        HdfsUtils.mkdir(yarnConfiguration, sourceFilePath);
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, avroStream, sourceFilePath + "/file.avro");
        csvUrl = ClassLoader.getSystemResource("com/latticeengines/eai/service/impl/file/file2.csv");
    }

    @AfterClass(groups = "aws")
    public void cleanup() throws IOException {
        HdfsUtils.rmdir(yarnConfiguration, sourceFilePath);
        s3Service.cleanupPrefix(s3Bucket, targetS3Path);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "aws")
    public void exportMetadataAndDataAndWriteToHdfs() throws Exception {
        ExportContext ctx = new ExportContext(yarnConfiguration);

        ExportConfiguration fileExportConfig = new ExportConfiguration();
        fileExportConfig.setExportFormat(ExportFormat.AVRO);
        fileExportConfig.setExportDestination(ExportDestination.S3);
        fileExportConfig.setCustomerSpace(TEST_CUSTOMER);
        Table table = createFile(new File(csvUrl.getPath()).getParentFile(), "file2");
        Extract extract = new Extract();
        extract.setPath(sourceFilePath + "/file.avro");
        table.setExtracts(Collections.singletonList(extract));

        fileExportConfig.setTable(table);
        Map<String, String> props = new HashMap<>();
        props.put(ExportProperty.TARGET_DIRECTORY, targetS3Path);
        props.put(ExportProperty.TARGET_FILE_NAME, "file.avro");
        fileExportConfig.setProperties(props);
        s3ExportService.exportDataFromHdfs(fileExportConfig, ctx);

        ApplicationId appId = ctx.getProperty(ExportProperty.APPID, ApplicationId.class);
        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        Assert.assertTrue(s3Service.listObjects(s3Bucket, targetS3Path).size() >= 1);
    }

}
