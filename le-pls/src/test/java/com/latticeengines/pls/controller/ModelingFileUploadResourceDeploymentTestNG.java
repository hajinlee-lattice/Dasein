package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.GzipUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.AvailableDateFormat;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.pls.frontend.FieldValidation;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.repository.writer.SourceFileWriterRepository;

public class ModelingFileUploadResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String PATH = "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file1.csv";
    private static final String COMPRESSED_PATH = "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file1.csv.gz";

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private SourceFileWriterRepository sourceFileRepository;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        String tenantId = CustomerSpace.parse(mainTestTenant.getId()).getTenantId();
        HdfsUtils.rmdir(yarnConfiguration, String.format("/Pods/Default/Contracts/%s", tenantId));
    }

    @BeforeMethod
    public void beforeMethod() {
        sourceFileRepository.deleteAll();
    }

    private ResponseDocument<SourceFile> submitFile(boolean unnamed, String filePath, boolean compressed) {
        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        map.add("file", new ClassPathResource(filePath));
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);
        String displayName = filePath.substring(StringUtils.lastIndexOf(filePath, '/') + 1);

        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<>(
                map, headers);
        if (unnamed) {
            String uri = String.format("/pls/models/uploadfile/unnamed?schema=%s&compressed=%s&displayName=%s&entity=%s",
                    SchemaInterpretation.SalesforceAccount, compressed, displayName, BusinessEntity.Account);
            ResponseEntity<String> result = restTemplate.exchange(getRestAPIHostPort() + uri, HttpMethod.POST,
                    requestEntity, String.class);
            return JsonUtils.deserialize(result.getBody(), new TypeReference<ResponseDocument<SourceFile>>() {
            });
        } else {
            String filename = DateTime.now().getMillis() + ".csv";
            String uri = String.format("/pls/models/uploadfile?fileName=%s&schema=%s&compressed=%s&displayName=%s&entity=%s",
                    filename, SchemaInterpretation.SalesforceAccount, compressed, displayName, BusinessEntity.Account);
            System.out.println(uri);
            ResponseEntity<String> result = restTemplate.exchange(getRestAPIHostPort() + uri, HttpMethod.POST,
                    requestEntity, String.class);
            return JsonUtils.deserialize(result.getBody(), new TypeReference<ResponseDocument<SourceFile>>() {
            });
        }
    }

    @Test(groups = "deployment")
    public void submitFileWithException() throws Exception {
        String pathForMisingFields = "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file_missing_required_fields.csv";
        boolean thrown = false;
        try {
            submitFile(false, pathForMisingFields, false);
        } catch (Exception e) {
            thrown = true;
            assertTrue(e instanceof RuntimeException);
        } finally {
            assertTrue(thrown);
        }

        String pathForEmptyHeader = "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file_empty_header.csv";
        thrown = false;
        try {
            submitFile(false, pathForEmptyHeader, false);
        } catch (Exception e) {
            thrown = true;
            assertTrue(e instanceof RuntimeException);
        } finally {
            assertTrue(thrown);
        }
    }

    @Test(groups = "deployment")
    public void uploadFile() throws Exception {
        switchToExternalUser();
        ResponseDocument<SourceFile> response = submitFile(false, PATH, false);
        assertTrue(response.isSuccess());
        SourceFile fileResponse = new ObjectMapper().convertValue(response.getResult(), SourceFile.class);
        String contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, fileResponse.getPath());
        String expectedContents = FileUtils.readFileToString(new File(ClassLoader.getSystemResource(PATH).getPath()), //
                Charset.defaultCharset());
        assertEquals(contents, expectedContents);

        List<SourceFile> files = sourceFileRepository.findAll();
        String path = fileResponse.getPath();
        foundTheFiles(path, files);
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "deployment")
    public void testValidations() {
        switchToExternalAdmin();
        ResponseDocument<SourceFile> response = submitFile(false, PATH, false);
        SourceFile fileResponse = JsonUtils.convertValue(response.getResult(), SourceFile.class);
        String fileName = fileResponse.getName();
        assertTrue(response.isSuccess());
        String getFieldMappingAPI = String.format(
                "/pls/models/uploadfile/%s/fieldmappings?entity=Account&feedType=DefaultSystem_AccountData&source=File",
                fileName);
        HttpEntity entity = new HttpEntity<>(null, null);
        ResponseEntity<String> result = restTemplate.exchange(getRestAPIHostPort() + getFieldMappingAPI,
                HttpMethod.POST, entity, String.class);
        ResponseDocument<FieldMappingDocument> responseDcoument = JsonUtils.deserialize(result.getBody(),
                new TypeReference<ResponseDocument<FieldMappingDocument>>() {
                });
        FieldMappingDocument fieldDocument = responseDcoument.getResult();
        fieldDocument.setIgnoredFields(Arrays.asList("Is Closed"));

        String validateAPI = String.format(
                "/pls/models/uploadfile/validate?displayName=%s&entity=Account&feedType=DefaultSystem_AccountData&source=File",
                fileName);
        entity = new HttpEntity<>(fieldDocument);
        ResponseEntity<List> list = restTemplate.exchange(getRestAPIHostPort() + validateAPI, HttpMethod.POST,
                entity, List.class);
        List<FieldValidation> validations = JsonUtils.convertList(list.getBody(), FieldValidation.class);
        // verify normal file can pass validation, no warning or error
        Assert.assertNotNull(validations);
        Assert.assertEquals(validations.size(), 0);

    }

    @Test(groups = "deployment")
    public void uploadUnnamedFile() throws Exception {
        switchToExternalAdmin();
        ResponseDocument<SourceFile> response = submitFile(true, PATH, false);
        assertTrue(response.isSuccess());
        String path = response.getResult().getPath();
        String contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, path);
        String expectedContents = FileUtils.readFileToString(new File(ClassLoader.getSystemResource(PATH).getPath()), //
                Charset.defaultCharset());
        assertEquals(contents, expectedContents);

        List<SourceFile> files = sourceFileRepository.findAll();
        foundTheFiles(path, files);
    }

    @Test(groups = "deployment")
    public void uploadCompressedFile() throws Exception {
        switchToExternalAdmin();

        ResponseDocument<SourceFile> response = submitFile(false, COMPRESSED_PATH, true);
        assertTrue(response.isSuccess());
        String path = response.getResult().getPath();
        String contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, path);

        String expectedContents = GzipUtils.decompressFileToString(ClassLoader.getSystemResource(COMPRESSED_PATH)
                .getPath());
        assertEquals(contents, expectedContents);

        List<SourceFile> files = sourceFileRepository.findAll();
        foundTheFiles(path, files);
    }

    @Test(groups = "deployment")
    public void testGetAvailableFormat() {
        switchToExternalAdmin();
        String uri = "/pls/models/uploadfile/dateformat";
        ResponseEntity<String> result = restTemplate.exchange(getRestAPIHostPort() + uri, HttpMethod.GET,
                null, String.class);
        ResponseDocument<AvailableDateFormat> responseDocument = JsonUtils.deserialize(result.getBody(),
                new TypeReference<ResponseDocument<AvailableDateFormat>>() {
        });
        Assert.assertNotNull(responseDocument);
        Assert.assertTrue(responseDocument.isSuccess());
        AvailableDateFormat availableDateFormat = responseDocument.getResult();
        Assert.assertNotNull(availableDateFormat);
        Assert.assertTrue(availableDateFormat.getDateFormats().contains("MM/DD/YYYY"));
        Assert.assertTrue(availableDateFormat.getTimeFormats().contains("00:00:00 12H"));
        Assert.assertTrue(availableDateFormat.getTimezones().contains("UTC"));
    }

    private void foundTheFiles(String path, List<SourceFile> files) {
        boolean found = false;
        for (SourceFile file : files) {
            if (file.getPath().equals(path)) {
                String[] split = path.split("/");
                assertEquals(file.getName(), split[split.length - 1]);
                found = true;
            }
        }
        assertTrue(found);
    }

}
