package com.latticeengines.apps.dcp.controller;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.retry.support.RetryTemplate;
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
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadRequest;
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
    private String uploadId;

    @BeforeClass(groups = {"deployment"})
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = {"deployment"})
    public void testCRUD() {
        String userId = "test@dnb.com";
        ProjectDetails details = projectService.createProject(mainCustomerSpace, "TestDCPProject",
                Project.ProjectType.Type1, userId, getPurposeOfUse(), null);
        projectId = details.getProjectId();

        InputStream specStream = testArtifactService.readTestArtifactAsStream(TEST_TEMPLATE_DIR, TEST_TEMPLATE_VERSION,
                TEST_TEMPLATE_NAME);
        FieldDefinitionsRecord fieldDefinitionsRecord = JsonUtils.deserialize(specStream, FieldDefinitionsRecord.class);
        Source source = sourceService.createSource(mainCustomerSpace, "TestSource", projectId,
                fieldDefinitionsRecord);
        sourceId = source.getSourceId();

        UploadRequest uploadRequest = new UploadRequest();
        UploadConfig config = new UploadConfig();
        uploadRequest.setUploadConfig(config);
        uploadRequest.setUserId(userId);
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(mainCustomerSpace);
        String dropFolder = UploadS3PathBuilderUtils.getDropFolder(dropBoxSummary.getDropBox());
        String uploadDir = UploadS3PathBuilderUtils.getUploadRoot(details.getProjectId(), source.getSourceId());
        String uploadDirKey = UploadS3PathBuilderUtils.combinePath(false, true, dropFolder, uploadDir);

        String uploadTS = "2020-03-20";
        String errorPath = uploadDirKey + uploadTS + "/error/file1.csv";
        String resultsPath = uploadDirKey + uploadTS + "/results/";
        String rawPath = uploadDirKey + uploadTS + "/raw/file2.csv";
        config.setUploadImportedErrorFilePath(errorPath);
        UploadDetails upload = uploadProxy.createUpload(mainCustomerSpace, source.getSourceId(), uploadRequest);
        Assert.assertEquals(upload.getStatus(), Upload.Status.NEW);
        Assert.assertEquals(upload.getCreatedBy(), userId);
        UploadConfig returnedConfig = upload.getUploadConfig();
        Assert.assertEquals(returnedConfig.getUploadImportedErrorFilePath(), errorPath);
        Assert.assertNull(returnedConfig.getUploadImportedFilePath());
        Assert.assertNull(returnedConfig.getUploadRawFilePath());

        // update config
        config.setUploadRawFilePath(rawPath);
        config.setUploadMatchResultPrefix(resultsPath);
        config.setUploadTimestamp(uploadTS);
        uploadProxy.updateUploadConfig(mainCustomerSpace, upload.getUploadId(), config);

        RetryTemplate retry = RetryUtils.getRetryTemplate(5,  Collections.singleton(AssertionError.class), null);
        retry.execute(context -> {
            List<UploadDetails> uploads = uploadProxy.getUploads(mainCustomerSpace,
            source.getSourceId(), null, Boolean.TRUE, 0, 20);
            Assert.assertNotNull(uploads);
            Assert.assertEquals(uploads.size(), 1);
            UploadDetails retrievedUpload = uploads.get(0);
            UploadConfig retrievedConfig = retrievedUpload.getUploadConfig();
            Assert.assertEquals(retrievedConfig.getUploadMatchResultPrefix(), resultsPath);
            Assert.assertEquals(retrievedConfig.getUploadTimestamp(), uploadTS);
            Assert.assertEquals(retrievedConfig.getUploadImportedErrorFilePath(), errorPath);
            Assert.assertEquals(retrievedConfig.getUploadRawFilePath(), rawPath);
            return true;
        });

        uploadProxy.updateUploadStatus(mainCustomerSpace, upload.getUploadId(), Upload.Status.MATCH_STARTED, null);
        retry.execute(context -> {
            List<UploadDetails>  uploads = uploadProxy.getUploads(mainCustomerSpace, source.getSourceId(),
                    Upload.Status.MATCH_STARTED, Boolean.FALSE, 0, 20);
            Assert.assertNotNull(uploads);
            Assert.assertEquals(uploads.size(), 1);
            UploadDetails retrievedUpload = uploads.get(0);
            Assert.assertEquals(retrievedUpload.getStatus(), Upload.Status.MATCH_STARTED);
            return true;
        });
        // create another upload
        UploadConfig config2 = new UploadConfig();
        UploadRequest uploadRequest2 = new UploadRequest();
        uploadRequest2.setUserId(userId);
        uploadRequest2.setUploadConfig(config2);
        String uploadTS2 = "2020-04-21-02-02-52.645";
        String rawPath2 = uploadDirKey + uploadTS2 + "/raw/file3.csv";
        config2.setUploadRawFilePath(rawPath2);
        uploadProxy.createUpload(mainCustomerSpace, source.getSourceId(), uploadRequest2);

        Boolean hasUnterminalUploads = uploadProxy.hasUnterminalUploads(mainCustomerSpace, upload.getUploadId());
        Assert.assertTrue(hasUnterminalUploads);
    }


    @SuppressWarnings("unchecked")
    @Test(groups = "deployment", dependsOnMethods = "testCRUD")
    public void testDownloadAll() throws Exception {
        List<UploadDetails> uploads = uploadProxy.getUploads(mainCustomerSpace, sourceId, Upload.Status.MATCH_STARTED, Boolean.TRUE, 0, 20);
        Assert.assertNotNull(uploads);
        Assert.assertEquals(uploads.size(), 1);
        UploadDetails upload = uploads.get(0);
        uploadId = upload.getUploadId();
        Assert.assertEquals(upload.getStatus(), Upload.Status.MATCH_STARTED);
        logger.info(JsonUtils.pprint(upload));

        StringInputStream sis = new StringInputStream("file1");

        DropBoxSummary dropBox = dropBoxProxy.getDropBox(mainCustomerSpace);
        String bucket = dropBox.getBucket();
        s3Service.uploadInputStream(bucket, upload.getUploadConfig().getUploadImportedErrorFilePath(), sis, true);

        StringInputStream sis2 = new StringInputStream("accepted");
        s3Service.uploadInputStream(bucket, upload.getUploadConfig().getUploadMatchResultAccepted(), sis2, true);

        StringInputStream sis3 = new StringInputStream("file2");
        s3Service.uploadInputStream(bucket, upload.getUploadConfig().getUploadRawFilePath(), sis3, true);

        StringInputStream sis4 = new StringInputStream("rejected");
        s3Service.uploadInputStream(bucket, upload.getUploadConfig().getUploadMatchResultRejected(), sis4, true);

        StringInputStream sis5 = new StringInputStream("processing_errors");
        s3Service.uploadInputStream(bucket, upload.getUploadConfig().getUploadMatchResultErrored(), sis5, true);

        // drop file to another upload
        List<UploadDetails> uploads2 = uploadProxy.getUploads(mainCustomerSpace, sourceId, Upload.Status.NEW, Boolean.TRUE, 0, 20);
        Assert.assertNotNull(uploads2);
        Assert.assertEquals(uploads2.size(), 1);
        UploadDetails upload2 = uploads2.get(0);
        StringInputStream sis6 = new StringInputStream("file3");
        s3Service.uploadInputStream(bucket, upload2.getUploadConfig().getUploadRawFilePath(), sis6, true);

        RestTemplate template = testBed.getRestTemplate();
        String tokenUrl = String.format("%s/pls/uploads/uploadId/%s/token", deployedHostPort,
                upload.getUploadId());
        String token = template.getForObject(tokenUrl, String.class);
        String downloadUrl = String.format("%s/pls/filedownloads/%s", deployedHostPort, token);
        SleepUtils.sleep(1000L);
        // test the result file can be extracted to 3 files
        ZipFile zipFile = downloadZipAndVerifyName(template, downloadUrl, "file2_Results.zip");
        Assert.assertNotNull(zipFile);
        List<FileHeader> fileHeaders = zipFile.getFileHeaders();
        Assert.assertNotNull(fileHeaders);
        Assert.assertEquals(fileHeaders.size(), 5);
        String tmp = System.getProperty("java.io.tmpdir");
        if (tmp.endsWith("/")) {
            tmp = tmp.substring(0, tmp.length() - 1);
        }
        String destPath = tmp + NamingUtils.uuid("/download");
        zipFile.extractAll(destPath);
        File destDir = new File(destPath);
        Assert.assertTrue(destDir.isDirectory());
        String[] files = destDir.list();
        Assert.assertEquals(ArrayUtils.getLength(files), 5);

        // Check the correct file names for each
        List<String> fileNames = Arrays.asList(destDir.list());
        Assert.assertTrue(fileNames.contains("file2.csv"));
        Assert.assertTrue(fileNames.contains("file2_Ingestion_Errors.csv"));
        Assert.assertTrue(fileNames.contains("file2_Matched.csv"));
        Assert.assertTrue(fileNames.contains("file2_Unmatched.csv"));
        Assert.assertTrue(fileNames.contains("file2_Processing_Errors.csv"));
        FileUtils.forceDelete(destDir);
    }

    @Test(groups = "deployment", dependsOnMethods = "testDownloadAll")
    public void testDownloadSelect() throws Exception {
        RestTemplate template = testBed.getRestTemplate();
        String tokenUrlBase = String.format("%s/pls/uploads/uploadId/%s/token", deployedHostPort, uploadId);

        for (int i = 1; i <= 31; ++i) {
            boolean includeRaw = (i % 2 == 1);
            boolean includeMatched = ((i / 2) % 2 == 1);
            boolean includeUnmatched = ((i / 4) % 2 == 1);
            boolean includeErrors = ((i / 8) % 2 == 1);
            boolean includeProcessing = ((i / 16) % 2 == 1);

            Boolean[] includes = {includeRaw, includeMatched, includeUnmatched, includeErrors, includeProcessing};
            int numFiles = (int) Arrays.stream(includes).filter(Boolean::booleanValue).count();

            String downloadParams = "";
            if (includeRaw) {
                downloadParams = downloadParams + ",RAW";
            }
            if (includeMatched) {
                downloadParams = downloadParams + ",MATCHED";
            }
            if (includeUnmatched) {
                downloadParams = downloadParams + ",UNMATCHED";
            }
            if (includeErrors) {
                downloadParams = downloadParams + ",IMPORT_ERRORS";
            }
            if (includeProcessing) {
                downloadParams = downloadParams + ",PROCESS_ERRORS";
            }
            downloadParams = downloadParams.substring(1);

            String tokenUrlFull = tokenUrlBase + "?files=" + downloadParams;
            String token = template.getForObject(tokenUrlFull, String.class);
            SleepUtils.sleep(300);
            String downloadUrl = String.format("%s/pls/filedownloads/%s", deployedHostPort, token);

            ZipFile zipFile = downloadZipFile(template, downloadUrl);
            List<FileHeader> fileHeaders = zipFile.getFileHeaders();
            Assert.assertEquals(fileHeaders.size(), numFiles);
            List<String> fileNames = fileHeaders.stream().map(FileHeader::getFileName).collect(Collectors.toList());

            // check each file's existence/omission
            Assert.assertEquals(includeRaw, fileNames.contains("file2.csv"));
            Assert.assertEquals(includeMatched, fileNames.contains("file2_Matched.csv"));
            Assert.assertEquals(includeUnmatched, fileNames.contains("file2_Unmatched.csv"));
            Assert.assertEquals(includeErrors, fileNames.contains("file2_Ingestion_Errors.csv"));
            Assert.assertEquals(includeProcessing, fileNames.contains("file2_Processing_Errors.csv"));
        }
    }

    public static ZipFile downloadZipFile(RestTemplate client, String downloadUrl) throws Exception {
        return downloadZipAndVerifyName(client, downloadUrl, null);
    }

    public static ZipFile downloadZipAndVerifyName(RestTemplate client, String downloadUrl, String fileName) throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(MediaType.ALL));
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<byte[]> response = client.exchange(downloadUrl, HttpMethod.GET, entity, byte[].class);
        byte[] contents = response.getBody();
        Assert.assertNotNull(contents);
        Assert.assertTrue(contents.length > 0);

        if (fileName != null) {
            String contentHeader = response.getHeaders().getFirst("Content-Disposition");
            Assert.assertTrue(StringUtils.isNotEmpty(contentHeader) && contentHeader.contains("filename=\"" + fileName + "\""));
        }

        // write the byte to zip
        File zip = File.createTempFile(NamingUtils.uuid("zip"), ".zip");
        zip.deleteOnExit();
        OutputStream outputStream = new FileOutputStream(zip);
        outputStream.write(contents);
        outputStream.flush();
        outputStream.close();

        return new ZipFile(zip);
    }
}
