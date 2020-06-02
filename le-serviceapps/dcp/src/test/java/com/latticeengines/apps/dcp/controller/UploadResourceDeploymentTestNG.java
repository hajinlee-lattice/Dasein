package com.latticeengines.apps.dcp.controller;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
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
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.util.UploadS3PathBuilderUtils;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.model.FileHeader;


public class UploadResourceDeploymentTestNG extends DCPDeploymentTestNGBase {

    @Inject
    private ProjectService projectService;

    @Inject
    private SourceService sourceService;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    private S3Service s3Service;

    private String sourceId;
    private String projectId;

    @BeforeClass(groups = {"deployment"})
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = {"deployment"})
    public void testCRUD() {
        ProjectDetails details = projectService.createProject(mainCustomerSpace, "TestDCPProject",
                Project.ProjectType.Type1, "test@dnb.com");
        projectId = details.getProjectId();

        InputStream specStream = testArtifactService.readTestArtifactAsStream(TEST_TEMPLATE_DIR, TEST_TEMPLATE_VERSION,
                TEST_TEMPLATE_NAME);
        FieldDefinitionsRecord fieldDefinitionsRecord = JsonUtils.deserialize(specStream, FieldDefinitionsRecord.class);
        Source source = sourceService.createSource(mainCustomerSpace, "TestSource", projectId,
                fieldDefinitionsRecord);
        sourceId = source.getSourceId();

        UploadConfig config = new UploadConfig();
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(mainCustomerSpace);
        String dropFolder = UploadS3PathBuilderUtils.getDropFolder(dropBoxSummary.getDropBox());
        String uploadDir = UploadS3PathBuilderUtils.getUploadRoot(details.getProjectId(), source.getSourceId());
        String uploadDirKey = UploadS3PathBuilderUtils.combinePath(false, true, dropFolder, uploadDir);

        String uploadTS = "2020-03-20";
        String errorPath = uploadDirKey + uploadTS + "/error/file1.csv";
        String importedFilePath = uploadDirKey + uploadTS + "/processed/file2.csv";
        String rawPath = uploadDirKey + uploadTS + "/raw/file3.csv";
        config.setUploadImportedErrorFilePath(errorPath);
        UploadDetails upload = uploadProxy.createUpload(mainCustomerSpace, source.getSourceId(), config);
        Assert.assertEquals(upload.getUploadStatus().getStatus(), Upload.Status.NEW);
        UploadConfig returnedConfig = upload.getUploadConfig();
        Assert.assertEquals(returnedConfig.getUploadImportedErrorFilePath(), errorPath);
        Assert.assertNull(returnedConfig.getUploadImportedFilePath());
        Assert.assertNull(returnedConfig.getUploadRawFilePath());

        // update config
        config.setUploadRawFilePath(rawPath);
        config.setUploadImportedFilePath(importedFilePath);
        config.setUploadTSPrefix(uploadTS);
        uploadProxy.updateUploadConfig(mainCustomerSpace, upload.getUploadId(), config);
        List<UploadDetails> uploads = uploadProxy.getUploads(mainCustomerSpace, source.getSourceId(), null);
        Assert.assertNotNull(uploads);
        Assert.assertEquals(uploads.size(), 1);
        UploadDetails retrievedUpload = uploads.get(0);
        UploadConfig retrievedConfig = retrievedUpload.getUploadConfig();
        Assert.assertEquals(retrievedConfig.getUploadImportedFilePath(), importedFilePath);
        Assert.assertEquals(retrievedConfig.getUploadTSPrefix(), uploadTS);
        Assert.assertEquals(retrievedConfig.getUploadImportedErrorFilePath(), errorPath);
        Assert.assertEquals(retrievedConfig.getUploadRawFilePath(), rawPath);

        uploadProxy.updateUploadStatus(mainCustomerSpace, upload.getUploadId(), Upload.Status.MATCH_STARTED);
        uploads = uploadProxy.getUploads(mainCustomerSpace, source.getSourceId(), Upload.Status.MATCH_STARTED);
        Assert.assertNotNull(uploads);
        Assert.assertEquals(uploads.size(), 1);
        retrievedUpload = uploads.get(0);
        Assert.assertEquals(retrievedUpload.getUploadStatus().getStatus(), Upload.Status.MATCH_STARTED);

        // create another upload
        UploadConfig config2 = new UploadConfig();
        String uploadTS2 = "2020-04-21-02-02-52.645";
        String rawPath2 = uploadDirKey + uploadTS2 + "/raw/file3.csv";
        config2.setUploadRawFilePath(rawPath2);
        uploadProxy.createUpload(mainCustomerSpace, source.getSourceId(), config2);
    }


    @Test(groups = "deployment", dependsOnMethods = "testCRUD")
    public void testDownload() throws Exception {
        List<UploadDetails> uploads = uploadProxy.getUploads(mainCustomerSpace, sourceId, Upload.Status.MATCH_STARTED);
        Assert.assertNotNull(uploads);
        Assert.assertEquals(uploads.size(), 1);
        UploadDetails upload = uploads.get(0);
        Assert.assertEquals(upload.getUploadStatus().getStatus(), Upload.Status.MATCH_STARTED);

        StringInputStream sis = new StringInputStream("file1");

        DropBoxSummary dropBox = dropBoxProxy.getDropBox(mainCustomerSpace);
        String bucket = dropBox.getBucket();
        s3Service.uploadInputStream(bucket, upload.getUploadConfig().getUploadImportedErrorFilePath(), sis, true);

        StringInputStream sis2 = new StringInputStream("file2");
        s3Service.uploadInputStream(bucket, upload.getUploadConfig().getUploadImportedFilePath(), sis2, true);

        StringInputStream sis3 = new StringInputStream("file3");
        s3Service.uploadInputStream(bucket, upload.getUploadConfig().getUploadRawFilePath(), sis3, true);

        // drop file to another upload
        List<UploadDetails> uploads2 = uploadProxy.getUploads(mainCustomerSpace, sourceId, Upload.Status.NEW);
        Assert.assertNotNull(uploads2);
        Assert.assertEquals(uploads2.size(), 1);
        UploadDetails upload2 = uploads2.get(0);
        StringInputStream sis4 = new StringInputStream("file4");
        s3Service.uploadInputStream(bucket, upload2.getUploadConfig().getUploadRawFilePath(), sis4, true);

        RestTemplate template = testBed.getRestTemplate();
        String tokenUrl = String.format("%s/pls/uploads/uploadId/%s/token", deployedHostPort,
                upload.getUploadId().toString());
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

        // write the byte to zip, then unzip the zip
        File zip = File.createTempFile(NamingUtils.uuid("zip"), ".zip");
        zip.deleteOnExit();
        OutputStream outputStream = new FileOutputStream(zip);
        outputStream.write(contents);
        outputStream.flush();
        outputStream.close();

        // test the zip file can be extracted to 3 files
        ZipFile zipFile = new ZipFile(zip);
        List<FileHeader> fileHeaders = zipFile.getFileHeaders();
        Assert.assertNotNull(fileHeaders);
        Assert.assertEquals(fileHeaders.size(), 3);
        String tmp = System.getProperty("java.io.tmpdir");
        if (tmp.endsWith("/")) {
            tmp = tmp.substring(0, tmp.length() -1);
        }
        String destPath = tmp + NamingUtils.uuid("/download");
        zipFile.extractAll(destPath);
        File destDir = new File(destPath);
        Assert.assertTrue(destDir.isDirectory());
        String[] files = destDir.list();
        Assert.assertEquals(ArrayUtils.getLength(files), 3);
        FileUtils.forceDelete(destDir);
    }

}
