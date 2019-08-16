package com.latticeengines.pls.workflow;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.MetadataSegmentExportService;
import com.latticeengines.proxy.exposed.dataplatform.JobProxy;
import com.latticeengines.testframework.exposed.proxy.pls.TestMetadataSegmentProxy;
import com.latticeengines.testframework.service.impl.TestPlayCreationHelper;

public class SegmentExportWorkflowDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(SegmentExportWorkflowDeploymentTestNG.class);

    @Autowired
    private TestMetadataSegmentProxy testMetadataSegmentProxy;

    @Autowired
    private TestPlayCreationHelper testPlayCreationHelper;

    @Autowired
    private JobProxy jobProxy;

    @Autowired
    private MetadataSegmentExportService metadataSegmentExportService;

    @Autowired
    private S3Service s3Service;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    private Tenant tenant;

    private CustomerSpace customerSpace;

    private HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder();


    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        testPlayCreationHelper.setupTenantAndData();
        testPlayCreationHelper.setupTestSegment();
        testPlayCreationHelper.setupTestRulesBasedModel();
        tenant = testPlayCreationHelper.getTenant();
        customerSpace = CustomerSpace.parse(tenant.getId());
        testPlayCreationHelper.getDeploymentTestBed().attachProtectedProxy(testMetadataSegmentProxy);
        log.info(String.format("Current tenant in test environment is %s", customerSpace));
    }

    @Test(groups = "deployment")
    public void testAccountAndContactExport() throws Exception {
        MetadataSegmentExport segmentExport = createExportJob(AtlasExportType.ACCOUNT_AND_CONTACT);
        log.info(String.format("Waiting for appId: %s", segmentExport.getApplicationId()));
        Assert.assertNotNull(segmentExport.getApplicationId());
        Assert.assertNotNull(segmentExport.getExportId());
        JobStatus status;
        int maxTries = 60 * 12; // Wait maximum 2 hours
        int i = 0;
        do {
            status = jobProxy.getJobStatus(segmentExport.getApplicationId());
            try {
                Thread.sleep(10000L);
            } catch (InterruptedException e) {
                // Do nothing for InterruptedException
                log.info(e.getMessage());
            }
            i++;
            if (i == maxTries) {
                break;
            }
        } while (!YarnUtils.TERMINAL_STATUS.contains(status.getStatus()));
        Assert.assertEquals(status.getStatus(), FinalApplicationStatus.SUCCEEDED);
        verifyTest(segmentExport.getExportId());
    }

    private MetadataSegmentExport createExportJob(AtlasExportType type) {
        String segmentName = UUID.randomUUID().toString();
        MetadataSegmentExport metadataSegmentExport = new MetadataSegmentExport();
        metadataSegmentExport.setType(type);
        metadataSegmentExport
                .setAccountFrontEndRestriction(testPlayCreationHelper.getSegment().getAccountFrontEndRestriction());
        metadataSegmentExport
                .setContactFrontEndRestriction(testPlayCreationHelper.getSegment().getContactFrontEndRestriction());
        metadataSegmentExport.setStatus(MetadataSegmentExport.Status.RUNNING);
        metadataSegmentExport.setExportPrefix(segmentName);
        metadataSegmentExport.setCreatedBy("CREATED_BY");
        metadataSegmentExport.setCleanupBy(new Date(System.currentTimeMillis() + 7 * 24 * 60 * 60 * 1000));

        metadataSegmentExport = testMetadataSegmentProxy.createSegmentExport(metadataSegmentExport, false);

        Assert.assertNotNull(metadataSegmentExport.getExportId());
        Assert.assertNotNull(metadataSegmentExport);
        Assert.assertNotNull(metadataSegmentExport.getExportId());
        return metadataSegmentExport;
    }

    private void verifyTest(String exportId) throws URISyntaxException {
        MetadataSegmentExport metadataSegmentExport =
                metadataSegmentExportService.getSegmentExportByExportId(exportId);
        String filePath = metadataSegmentExportService.getExportedFilePath(metadataSegmentExport);
        boolean hasAccountContactCsv = false;
        URI uri = new URI(filePath);
        String inputPath = uri.getPath();
        String s3FilePath = pathBuilder.exploreS3FilePath(inputPath, s3Bucket);
        String s3FileKey = s3FilePath.substring(pathBuilder.getS3BucketDir(s3Bucket).length() + 1);
        Assert.assertTrue(s3Service.objectExist(s3Bucket, s3FileKey));
        if (metadataSegmentExport.getFileName().contains("ACCOUNT_AND_CONTACT")) {
            hasAccountContactCsv = true;
            InputStream csvStream = s3Service.readObjectAsStream(s3Bucket, s3FileKey);
            verifyAccountCsv(csvStream);
        }
        Assert.assertTrue(hasAccountContactCsv, "Cannot find Account csv in s3 folder.");
    }

    private void verifyAccountCsv(InputStream s3Stream) {
        try {
            Reader in = new InputStreamReader(s3Stream);
            CSVParser records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
            Map<String, Integer> headerMap = records.getHeaderMap();
            Assert.assertTrue(headerMap.containsKey("CEO Name"), "Header map: " + JsonUtils.serialize(headerMap));
            Assert.assertTrue(headerMap.containsKey("CEO Title"), "Header map: " + JsonUtils.serialize(headerMap));
            Assert.assertEquals(records.getRecords().size(), 235);

            for (CSVRecord record : records) {
                String id = record.get("ID");
                Assert.assertEquals(id, "0012400001DNRjXAAX");
                String city = record.get("City");
                Assert.assertEquals(city, "MCPHERSON");
                String industry = record.get("Industry");
                Assert.assertEquals(industry, "");
                String webSite = record.get("Website");
                Assert.assertEquals(webSite, "eprod.com");
                break; // only test the first record
            }
        } catch (IOException e) {
            Assert.fail("Failed to verify account csv.", e);
        }
    }

    @AfterClass(groups = {"deployment"})
    public void teardown() {
        testPlayCreationHelper.cleanupArtifacts(true);
    }
}
