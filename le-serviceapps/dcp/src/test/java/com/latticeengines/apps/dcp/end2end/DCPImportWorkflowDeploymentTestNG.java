package com.latticeengines.apps.dcp.end2end;

import java.io.InputStream;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.testframework.DCPDeploymentTestNGBase;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.dcp.DCPImportRequest;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectRequest;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceRequest;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadStats;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.util.UploadS3PathBuilderUtils;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.proxy.exposed.dcp.DCPProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class DCPImportWorkflowDeploymentTestNG extends DCPDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DCPImportWorkflowDeploymentTestNG.class);

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

    private ProjectDetails projectDetails;
    private Source source;
    private long uploadId;
    private String s3FileKey;

    @BeforeClass(groups = {"deployment"})
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

        List<Upload> uploadList = uploadProxy.getUploads(mainCustomerSpace, source.getSourceId(), null);
        Assert.assertNotNull(uploadList);
        Assert.assertEquals(uploadList.size(), 1);
        Upload upload = uploadList.get(0);
        uploadId = upload.getPid();

        verifyImport();
    }

    private void verifyImport() {
        Upload upload = uploadProxy.getUpload(mainCustomerSpace, uploadId);
        log.info(JsonUtils.serialize(upload));
        Assert.assertNotNull(upload);
        Assert.assertNotNull(upload.getStatus());

        Assert.assertEquals(upload.getStatus(), Upload.Status.FINISHED);

        Assert.assertFalse(StringUtils.isEmpty(upload.getUploadConfig().getDropFilePath()));
        Assert.assertFalse(StringUtils.isEmpty(upload.getUploadConfig().getUploadRawFilePath()));
        verifyMatchResult(upload);
        verifyUploadStats(upload);
    }

    private void prepareTenant() {
        // Create Project
        ProjectRequest projectRequest = new ProjectRequest();
        projectRequest.setDisplayName("ImportEnd2EndProject");
        projectRequest.setProjectType(Project.ProjectType.Type1);
        projectDetails = projectProxy.createDCPProject(mainCustomerSpace, projectRequest, "dcp_deployment@dnb.com");
        // Create Source
        InputStream specStream = testArtifactService.readTestArtifactAsStream(TEST_TEMPLATE_DIR, TEST_TEMPLATE_VERSION, TEST_TEMPLATE_NAME);
        FieldDefinitionsRecord fieldDefinitionsRecord = JsonUtils.deserialize(specStream, FieldDefinitionsRecord.class);
        SourceRequest sourceRequest = new SourceRequest();
        sourceRequest.setDisplayName("ImportEnd2EndSource");
        sourceRequest.setProjectId(projectDetails.getProjectId());
        sourceRequest.setFieldDefinitionsRecord(fieldDefinitionsRecord);
        source = sourceProxy.createSource(mainCustomerSpace, sourceRequest);
        // Copy test file to drop folder
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(mainCustomerSpace);
        String dropPath = UploadS3PathBuilderUtils.getDropRoot(projectDetails.getProjectId(), source.getSourceId());
        dropPath = UploadS3PathBuilderUtils.combinePath(false, true,
                UploadS3PathBuilderUtils.getDropFolder(dropBoxSummary.getDropBox()), dropPath);
        s3FileKey = dropPath + TEST_ACCOUNT_DATA_FILE;
        testArtifactService.copyTestArtifactFile(TEST_DATA_DIR, "2", TEST_ACCOUNT_DATA_FILE, s3Bucket, s3FileKey);
    }

    private void verifyUploadStats(Upload upload) {
        UploadStats uploadStats = upload.getStatistics();
        Assert.assertNotNull(uploadStats);

        UploadStats.ImportStats importStats = uploadStats.getImportStats();
        Assert.assertNotNull(importStats);
        Assert.assertTrue(importStats.getSuccessCnt() > 0);

        UploadStats.MatchStats matchStats = uploadStats.getMatchStats();
        Assert.assertNotNull(matchStats);
        Assert.assertTrue(matchStats.getAcceptedCnt() > 0);
    }

    private void verifyMatchResult(Upload upload) {
        String matchResultName = upload.getMatchResultName();
        Assert.assertNotNull(matchResultName);
        Table matchResult = metadataProxy.getTableSummary(mainCustomerSpace, matchResultName);
        Assert.assertNotNull(matchResult);
        Assert.assertEquals(matchResult.getExtracts().size(), 1);

        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(mainCustomerSpace);
        String dropFolder = UploadS3PathBuilderUtils.getDropFolder(dropBoxSummary.getDropBox());
        String acceptedPath = UploadS3PathBuilderUtils.combinePath(false, false,
                dropFolder, upload.getUploadConfig().getUploadMatchResultAccepted());
        String rejectedPath = UploadS3PathBuilderUtils.combinePath(false, false,
                dropFolder, upload.getUploadConfig().getUploadMatchResultRejected());
        String bucket = dropBoxSummary.getBucket();
        System.out.println("acceptedPath=" + acceptedPath);
        System.out.println("rejectedPath=" + rejectedPath);
        Assert.assertTrue(s3Service.objectExist(bucket, acceptedPath));
        Assert.assertTrue(s3Service.objectExist(bucket, rejectedPath));
    }

}
