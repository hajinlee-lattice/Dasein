package com.latticeengines.pls.controller;


import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;

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
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.pls.entitymanager.SourceFileEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

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
    
    private SimpleBooleanResponse submitFile() throws Exception {
        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        map.add("file", new ClassPathResource(PATH));
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<LinkedMultiValueMap<String, Object>>(
                map, headers);
        ResponseEntity<String> result = restTemplate.exchange(getRestAPIHostPort() + "/pls/fileuploads?name=file1.csv",
                HttpMethod.POST, requestEntity, String.class);
        return JsonUtils.deserialize(result.getBody(), SimpleBooleanResponse.class);
    }
    

    @Test(groups = "functional")
    public void uploadFile() throws Exception {
        switchToExternalAdmin();
        assertTrue(submitFile().isSuccess());
        String path = String.format( //
                "/Pods/Default/Contracts/%sPLSTenant1/Tenants/%sPLSTenant1/Spaces/Production/Data/Files/file1.csv", //
                contractId, contractId); 
        String contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, path);
        String expectedContents = FileUtils.readFileToString(new File(ClassLoader.getSystemResource(PATH).getPath()));
        assertEquals(contents, expectedContents);
        
        SourceFile sourceFile = sourceFileEntityMgr.findAll().get(0);
        assertEquals(sourceFile.getPath(), path);
        assertEquals(sourceFile.getName(), "file1.csv");
    }

    @Test(groups = "functional")
    public void uploadFileWithNoAccess() throws Exception {
        switchToExternalUser();
        boolean exception = false;
        try {
            submitFile().isSuccess();
        } catch (Exception e) {
            exception = true;
            assertEquals(e.getMessage(), "403");
        }
        assertTrue(exception, "Exception should have been thrown.");
    }
}
