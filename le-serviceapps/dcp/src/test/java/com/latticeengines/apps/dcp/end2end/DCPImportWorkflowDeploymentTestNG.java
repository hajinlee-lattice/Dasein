package com.latticeengines.apps.dcp.end2end;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.service.UploadService;
import com.latticeengines.apps.dcp.testframework.DCPDeploymentTestNGBase;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.dcp.DCPImportRequest;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectRequest;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceRequest;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadStats;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.util.UploadS3PathBuilderUtils;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.proxy.exposed.dcp.DCPProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

import au.com.bytecode.opencsv.CSVReader;

public class DCPImportWorkflowDeploymentTestNG extends DCPDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DCPImportWorkflowDeploymentTestNG.class);

    private static final String TEST_ACCOUNT_ERROR_FILE = "Account_dup_header.csv";

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    private DCPProxy dcpProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    @Inject
    private S3Service s3Service;

    @Inject
    private UploadService uploadService;

    private ProjectDetails projectDetails;
    private Source source;
    private String uploadId;
    private String s3FileKey;

    @BeforeClass(groups = { "deployment" })
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "deployment")
    public void testImport() {
        prepareTenant();

        DCPImportRequest request = new DCPImportRequest();
        request.setProjectId(projectDetails.getProjectId());
        request.setSourceId(source.getSourceId());
        request.setS3FileKey(s3FileKey);
        ApplicationId applicationId = dcpProxy.startImport(mainCustomerSpace, request);
        JobStatus completedStatus = waitForWorkflowStatus(applicationId.toString(), false);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);

        List<UploadDetails> uploadList = uploadProxy.getUploads(mainCustomerSpace, source.getSourceId(), null);
        Assert.assertNotNull(uploadList);
        Assert.assertEquals(uploadList.size(), 1);
        UploadDetails upload = uploadList.get(0);
        uploadId = upload.getUploadId();

        verifyImport();
    }

    @Test(groups = "deployment", dependsOnMethods = "testImport")
    public void testErrorImport() {

        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(mainCustomerSpace);
        String dropPath = UploadS3PathBuilderUtils.getDropRoot(projectDetails.getProjectId(), source.getSourceId());
        dropPath = UploadS3PathBuilderUtils.combinePath(false, true,
                UploadS3PathBuilderUtils.getDropFolder(dropBoxSummary.getDropBox()), dropPath);
        String errorFileKey = dropPath + TEST_ACCOUNT_ERROR_FILE;
        testArtifactService.copyTestArtifactFile(TEST_DATA_DIR, TEST_DATA_VERSION, TEST_ACCOUNT_ERROR_FILE, s3Bucket,
                errorFileKey);

        DCPImportRequest request = new DCPImportRequest();
        request.setProjectId(projectDetails.getProjectId());
        request.setSourceId(source.getSourceId());
        request.setS3FileKey(errorFileKey);
        ApplicationId applicationId = dcpProxy.startImport(mainCustomerSpace, request);
        JobStatus completedStatus = waitForWorkflowStatus(applicationId.toString(), false);
        Assert.assertEquals(completedStatus, JobStatus.FAILED);
        List<UploadDetails> uploadList = uploadProxy.getUploads(mainCustomerSpace, source.getSourceId(), null);
        Assert.assertNotNull(uploadList);
        Assert.assertEquals(uploadList.size(), 2);
        UploadDetails upload = uploadList.get(1);
        Assert.assertEquals(upload.getStatus(), Upload.Status.ERROR);
    }

    private void verifyImport() {
        UploadDetails upload = uploadProxy.getUploadByUploadId(mainCustomerSpace, uploadId);
        log.info(JsonUtils.serialize(upload));
        Assert.assertNotNull(upload);
        Assert.assertNotNull(upload.getStatus());

        Assert.assertEquals(upload.getStatus(), Upload.Status.FINISHED);

        Assert.assertFalse(StringUtils.isEmpty(upload.getUploadConfig().getDropFilePath()));
        Assert.assertFalse(StringUtils.isEmpty(upload.getUploadConfig().getUploadRawFilePath()));
        verifyErrorFile(upload);
        verifyMatchResult(upload);
        verifyUploadStats(upload);
        verifyDownload(upload);
    }

    private void verifyErrorFile(UploadDetails upload) {
        Assert.assertFalse(StringUtils.isEmpty(upload.getUploadConfig().getUploadImportedErrorFilePath()));
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(mainCustomerSpace);
        String dropFolder = UploadS3PathBuilderUtils.getDropFolder(dropBoxSummary.getDropBox());
        String errorFileKey = UploadS3PathBuilderUtils.combinePath(false, false, dropFolder,
                upload.getUploadConfig().getUploadImportedErrorFilePath());
        System.out.println("Error file path=" + errorFileKey);
        Assert.assertTrue(s3Service.objectExist(dropBoxSummary.getBucket(), errorFileKey));
        Assert.assertNotNull(upload.getStatistics().getImportStats().getErrorCnt());
        Assert.assertEquals(upload.getStatistics().getImportStats().getErrorCnt().longValue(), 5L);
    }

    private void prepareTenant() {
        // Create Project
        ProjectRequest projectRequest = new ProjectRequest();
        projectRequest.setDisplayName("ImportEnd2EndProject");
        projectRequest.setProjectType(Project.ProjectType.Type1);
        projectDetails = projectProxy.createDCPProject(mainCustomerSpace, projectRequest, "dcp_deployment@dnb.com");
        // Create Source
        InputStream specStream = testArtifactService.readTestArtifactAsStream(TEST_TEMPLATE_DIR, TEST_TEMPLATE_VERSION,
                TEST_TEMPLATE_NAME);
        FieldDefinitionsRecord fieldDefinitionsRecord = JsonUtils.deserialize(specStream, FieldDefinitionsRecord.class);
        SourceRequest sourceRequest = new SourceRequest();
        sourceRequest.setDisplayName("ImportEnd2EndSource");
        sourceRequest.setProjectId(projectDetails.getProjectId());
        sourceRequest.setFieldDefinitionsRecord(fieldDefinitionsRecord);
        source = sourceProxy.createSource(mainCustomerSpace, sourceRequest);
        // Pause this source for s3 import.
        sourceProxy.pauseSource(mainCustomerSpace, source.getSourceId());
        // Copy test file to drop folder
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(mainCustomerSpace);
        String dropPath = UploadS3PathBuilderUtils.getDropRoot(projectDetails.getProjectId(), source.getSourceId());
        dropPath = UploadS3PathBuilderUtils.combinePath(false, true,
                UploadS3PathBuilderUtils.getDropFolder(dropBoxSummary.getDropBox()), dropPath);
        s3FileKey = dropPath + TEST_ACCOUNT_DATA_FILE;
        testArtifactService.copyTestArtifactFile(TEST_DATA_DIR, TEST_DATA_VERSION, TEST_ACCOUNT_DATA_FILE, s3Bucket,
                s3FileKey);
    }

    private void verifyUploadStats(UploadDetails upload) {
        UploadStats uploadStats = upload.getStatistics();
        System.out.println(JsonUtils.serialize(uploadStats));
        Assert.assertNotNull(uploadStats);

        UploadStats.ImportStats importStats = uploadStats.getImportStats();
        Assert.assertNotNull(importStats);
        Assert.assertTrue(importStats.getSuccessCnt() > 0);

        UploadStats.MatchStats matchStats = uploadStats.getMatchStats();
        Assert.assertNotNull(matchStats);
        Assert.assertTrue(matchStats.getAcceptedCnt() > 0);

        Assert.assertEquals(Long.valueOf(matchStats.getAcceptedCnt() + //
                matchStats.getPendingReviewCnt() + matchStats.getRejectedCnt()), importStats.getSuccessCnt());
    }

    private void verifyMatchResult(UploadDetails upload) {
        String uploadId = upload.getUploadId();
        String matchResultName = uploadService.getMatchResultTableName(uploadId);
        Assert.assertNotNull(matchResultName);
        Table matchResult = metadataProxy.getTableSummary(mainCustomerSpace, matchResultName);
        Assert.assertNotNull(matchResult);
        Assert.assertEquals(matchResult.getExtracts().size(), 1);

        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(mainCustomerSpace);
        String dropFolder = UploadS3PathBuilderUtils.getDropFolder(dropBoxSummary.getDropBox());
        String acceptedPath = UploadS3PathBuilderUtils.combinePath(false, false, dropFolder,
                upload.getUploadConfig().getUploadMatchResultAccepted());
        String rejectedPath = UploadS3PathBuilderUtils.combinePath(false, false, dropFolder,
                upload.getUploadConfig().getUploadMatchResultRejected());
        String bucket = dropBoxSummary.getBucket();
        System.out.println("acceptedPath=" + acceptedPath);
        System.out.println("rejectedPath=" + rejectedPath);
        Assert.assertTrue(s3Service.objectExist(bucket, acceptedPath));
        Assert.assertTrue(s3Service.objectExist(bucket, rejectedPath));
        verifyCsvContent(bucket, acceptedPath);
        verifyCsvContent(bucket, rejectedPath);
    }

    private void verifyCsvContent(String bucket, String path) {
        InputStream is = s3Service.readObjectAsStream(bucket, path);
        InputStreamReader reader = new InputStreamReader(is);
        try (CSVReader csvReader = new CSVReader(reader)) {
            String[] nextRecord = csvReader.readNext();
            int count = 0;
            List<String> headers = new ArrayList<>();
            int nameIdx = -1;
            while (nextRecord != null && (count++) < 10) {
                if (count == 1) {
                    headers.addAll(Arrays.asList(nextRecord));
                    verifyOutputHeaders(headers);
                    nameIdx = headers.indexOf("Company Name");
                } else {
                    Assert.assertTrue(StringUtils.isNotBlank(nextRecord[nameIdx])); // Original Name is non-empty
                    nextRecord = csvReader.readNext();
                }
            }
        } catch (IOException e) {
            Assert.fail("Failed to read output csv", e);
        }
    }

    private void verifyOutputHeaders(List<String> headers) {
        System.out.println(headers);
        Assert.assertTrue(headers.contains("Company Name"));
        Assert.assertTrue(headers.contains("Test Date")); // in spec
        Assert.assertFalse(headers.contains("Test Date 2")); // not in spec
    }

    private void verifyDownload(UploadDetails upload) {
        RestTemplate template = testBed.getRestTemplate();
        String tokenUrl = String.format("%s/pls/uploads/uploadId/%s/token", deployedHostPort,
                upload.getUploadId());
        String token = template.getForObject(tokenUrl, String.class);
        SleepUtils.sleep(300);
        String downloadUrl = String.format("%s/pls/filedownloads/%s", deployedHostPort, token);
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(MediaType.ALL));
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<byte[]> response = template.exchange(downloadUrl, HttpMethod.GET, entity, byte[].class);
        String fileName = response.getHeaders().getFirst("Content-Disposition");
        Assert.assertTrue(StringUtils.isNotBlank(fileName) && fileName.contains(".zip"));
        byte[] contents = response.getBody();
        Assert.assertNotNull(contents);
        Assert.assertTrue(contents.length > 0);

    }

}
