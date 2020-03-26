package com.latticeengines.apps.dcp.end2end;

import java.io.InputStream;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.testframework.DCPDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.dcp.DCPImportRequest;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectRequest;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceRequest;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.util.UploadS3PathBuilderUtils;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.proxy.exposed.dcp.DCPProxy;

public class DCPImportWorkflowDeploymentTestNG extends DCPDeploymentTestNGBase {

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    private DCPProxy dcpProxy;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    private ProjectDetails projectDetails;
    private Source source;
    private Upload upload;
    private String s3FileKey;

    @BeforeClass(groups = {"deployment", "end2end"})
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "end2end")
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
        upload = uploadList.get(0);
    }

    private void validateImport() {
        Assert.assertNotNull(upload);
        Assert.assertNotNull(upload.getStatus());
        Assert.assertFalse(StringUtils.isEmpty(upload.getUploadConfig().getDropFilePath()));
        Assert.assertFalse(StringUtils.isEmpty(upload.getUploadConfig().getUploadRawFilePath()));
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
        testArtifactService.copyTestArtifactFile(TEST_DATA_DIR, TEST_DATA_VERSION, TEST_ACCOUNT_DATA_FILE, s3Bucket, s3FileKey);
    }
}
