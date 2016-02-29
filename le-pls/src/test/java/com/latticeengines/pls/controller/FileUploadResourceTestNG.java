package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.metadata.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.entitymanager.SourceFileEntityMgr;

public class FileUploadResourceTestNG extends PlsFunctionalTestNGBase {

    private static final String PATH = "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file1.csv";

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private SourceFileEntityMgr sourceFileEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, String.format("/Pods/Default/Contracts/%sPLSTenant1", contractId));
        setUpMarketoEloquaTestEnvironment();
    }

    @BeforeMethod
    public void beforeMethod() {
        sourceFileEntityMgr.deleteAll();
    }

    private ResponseDocument<SourceFile> submitFile(boolean unnamed) throws Exception {
        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        map.add("file", new ClassPathResource(PATH));
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<LinkedMultiValueMap<String, Object>>(
                map, headers);
        if (unnamed) {
            String path = String.format("/pls/fileuploads/unnamed?schema=%s", SchemaInterpretation.SalesforceAccount);
            ResponseEntity<String> result = restTemplate.exchange(getRestAPIHostPort() + path, HttpMethod.POST,
                    requestEntity, String.class);
            return JsonUtils.deserialize(result.getBody(), new TypeReference<ResponseDocument<SourceFile>>() {
            });
        } else {
            String path = String.format("/pls/fileuploads?fileName=file1.csv&schema=%s",
                    SchemaInterpretation.SalesforceAccount);
            ResponseEntity<String> result = restTemplate.exchange(getRestAPIHostPort() + path, HttpMethod.POST,
                    requestEntity, String.class);
            return JsonUtils.deserialize(result.getBody(), new TypeReference<ResponseDocument<SourceFile>>() {
            });
        }
    }


    @Test(groups = "functional")
    public void uploadFile() throws Exception {
        switchToExternalAdmin();
        ResponseDocument<SourceFile> response = submitFile(false);
        assertTrue(response.isSuccess());
        String path = String.format( //
                "/Pods/Default/Contracts/%sPLSTenant1/Tenants/%sPLSTenant1/Spaces/Production/Data/Files/file1.csv", //
                contractId, contractId);
        String contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, path);
        String expectedContents = FileUtils.readFileToString(new File(ClassLoader.getSystemResource(PATH).getPath()));
        assertEquals(contents, expectedContents);

        List<SourceFile> files = sourceFileEntityMgr.findAll();
        boolean found = false;
        for (SourceFile file : files) {
            if (file.getPath().equals(path)) {
                assertEquals(file.getName(), "file1.csv");
                found = true;
            }
        }

        assertTrue(found);
    }

    @Test(groups = "functional")
    public void uploadUnnamedFile() throws Exception {
        switchToExternalAdmin();
        ResponseDocument<SourceFile> response = submitFile(true);
        assertTrue(response.isSuccess());
        String path = response.getResult().getPath();
        String contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, path);
        String expectedContents = FileUtils.readFileToString(new File(ClassLoader.getSystemResource(PATH).getPath()));
        assertEquals(contents, expectedContents);

        List<SourceFile> files = sourceFileEntityMgr.findAll();
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

    @Test(groups = "functional")
    public void uploadFileWithNoAccess() throws Exception {
        switchToExternalUser();
        boolean exception = false;
        try {
            submitFile(false).isSuccess();
        } catch (Exception e) {
            exception = true;
            assertEquals(e.getMessage(), "403");
        }
        assertTrue(exception, "Exception should have been thrown.");
    }
}
