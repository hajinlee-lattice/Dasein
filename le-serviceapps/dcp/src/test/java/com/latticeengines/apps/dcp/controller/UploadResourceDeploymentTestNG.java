package com.latticeengines.apps.dcp.controller;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.util.StringInputStream;
import com.latticeengines.apps.dcp.service.ProjectService;
import com.latticeengines.apps.dcp.service.SourceService;
import com.latticeengines.apps.dcp.testframework.DCPDeploymentTestNGBase;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.util.UploadS3PathBuilderUtils;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;

public class UploadResourceDeploymentTestNG extends DCPDeploymentTestNGBase {

    @Inject
    private ProjectService projectService;

    @Inject
    private SourceService sourceService;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    private S3Service s3Service;

    private String SOURCE_ID;

    private String PROJECT_ID;

    @BeforeClass(groups = {"deployment"})
    public void setup() throws Exception {
        setupTestEnvironment();
    }

    @Test(groups = {"deployment"})
    public void testCRUD() {
        ProjectDetails details = projectService.createProject(mainCustomerSpace, "TestDCPProject",
                Project.ProjectType.Type1, "test@dnb.com");
        PROJECT_ID = details.getProjectId();

        InputStream specStream = testArtifactService.readTestArtifactAsStream(TEST_TEMPLATE_DIR, TEST_TEMPLATE_VERSION,
                TEST_TEMPLATE_NAME);
        FieldDefinitionsRecord fieldDefinitionsRecord = JsonUtils.deserialize(specStream, FieldDefinitionsRecord.class);
        Source source = sourceService.createSource(mainCustomerSpace, "TestSource", PROJECT_ID,
                fieldDefinitionsRecord);
        SOURCE_ID = source.getSourceId();

        UploadConfig config = new UploadConfig();
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(mainCustomerSpace);
        String dropFolder = UploadS3PathBuilderUtils.getDropFolder(dropBoxSummary.getDropBox());
        String uploadDir = UploadS3PathBuilderUtils.getUploadRoot(details.getProjectId(), source.getSourceId());
        String uploadDirKey = UploadS3PathBuilderUtils.combinePath(false, true, dropFolder, uploadDir);

        String timeStamp = "2020-03-20";
        String errorPath = uploadDirKey + timeStamp + "/error/file1.csv";
        String importedFilePath = uploadDirKey + timeStamp + "/processed/file2.csv";
        String rawPath = uploadDirKey + timeStamp + "/raw/file3.csv";
        config.setUploadImportedErrorFilePath(errorPath);
        Upload upload = uploadProxy.createUpload(mainCustomerSpace, source.getSourceId(), config);
        Assert.assertEquals(upload.getStatus(), Upload.Status.NEW);
        UploadConfig returnedConfig = upload.getUploadConfig();
        Assert.assertEquals(returnedConfig.getUploadImportedErrorFilePath(), errorPath);
        Assert.assertNull(returnedConfig.getUploadImportedFilePath());
        Assert.assertNull(returnedConfig.getUploadRawFilePath());

        // update config
        config.setUploadRawFilePath(rawPath);
        config.setUploadImportedFilePath(importedFilePath);
        config.setUploadTSPrefix(timeStamp);
        uploadProxy.updateUploadConfig(mainCustomerSpace, upload.getPid(), config);
        List<Upload> uploads = uploadProxy.getUploads(mainCustomerSpace, source.getSourceId(), null);
        Assert.assertNotNull(uploads);
        Assert.assertEquals(uploads.size(), 1);
        Upload retrievedUpload = uploads.get(0);
        UploadConfig retrievedConfig = retrievedUpload.getUploadConfig();
        Assert.assertEquals(retrievedConfig.getUploadImportedFilePath(), importedFilePath);
        Assert.assertEquals(retrievedConfig.getUploadTSPrefix(), timeStamp);
        Assert.assertEquals(retrievedConfig.getUploadImportedErrorFilePath(), errorPath);
        Assert.assertEquals(retrievedConfig.getUploadRawFilePath(), rawPath);

        uploadProxy.updateUploadStatus(mainCustomerSpace, upload.getPid(), Upload.Status.MATCH_STARTED);
        uploads = uploadProxy.getUploads(mainCustomerSpace, source.getSourceId(), Upload.Status.MATCH_STARTED);
        Assert.assertNotNull(uploads);
        Assert.assertEquals(uploads.size(), 1);
        retrievedUpload = uploads.get(0);
        Assert.assertEquals(retrievedUpload.getStatus(), Upload.Status.MATCH_STARTED);
    }


    @Test(groups = "deployment", dependsOnMethods = "testCRUD")
    public void testDownload() throws Exception {
        List<Upload> uploads = uploadProxy.getUploads(mainCustomerSpace, SOURCE_ID, Upload.Status.MATCH_STARTED);
        Assert.assertNotNull(uploads);
        Assert.assertEquals(uploads.size(), 1);
        Upload upload = uploads.get(0);
        Assert.assertEquals(upload.getStatus(), Upload.Status.MATCH_STARTED);

        StringInputStream sis = new StringInputStream("file1");

        DropBoxSummary dropBox = dropBoxProxy.getDropBox(mainCustomerSpace);
        String bucket = dropBox.getBucket();
        s3Service.uploadInputStream(bucket, upload.getUploadConfig().getUploadImportedErrorFilePath(), sis, true);

        StringInputStream sis2 = new StringInputStream("file2");
        s3Service.uploadInputStream(bucket, upload.getUploadConfig().getUploadImportedFilePath(), sis2, true);

        StringInputStream sis3 = new StringInputStream("file3");
        s3Service.uploadInputStream(bucket, upload.getUploadConfig().getUploadRawFilePath(), sis3, true);

        RestTemplate template = testBed.getRestTemplate();
        String url = String.format("%s/pls/uploads/uploadId/%s/download", deployedHostPort, upload.getPid().toString());
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Arrays.asList(MediaType.ALL));
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<byte[]> response = template.exchange(url, HttpMethod.GET, entity, byte[].class);
        String fileName = response.getHeaders().getFirst("Content-Disposition");
        Assert.assertTrue(fileName.contains(".zip"));
        byte[] contents = response.getBody();
        Assert.assertNotNull(contents);
        Assert.assertTrue(contents.length > 0);

    }

}
