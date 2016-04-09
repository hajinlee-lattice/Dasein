package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;
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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ZipUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.pls.entitymanager.SourceFileEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;

public class ModelingFileUploadResourceTestNG extends PlsFunctionalTestNGBaseDeprecated {

    private static final String PATH = "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file1.csv";
    private static final String COMPRESSED_PATH = "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file1.csv.zip";

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private SourceFileEntityMgr sourceFileEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, String.format("/Pods/Default/Contracts/%sPLSTenant1", contractId));
        setupMarketoEloquaTestEnvironment();
    }

    @BeforeMethod
    public void beforeMethod() {
        sourceFileEntityMgr.deleteAll();
    }

    private ResponseDocument<SourceFile> submitFile(boolean unnamed, String filePath, boolean compressed)
            throws Exception {
        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        map.add("file", new ClassPathResource(filePath));
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<LinkedMultiValueMap<String, Object>>(
                map, headers);
        if (unnamed) {
            String uri = String.format("/pls/models/fileuploads/unnamed?schema=%s&compressed=%s",
                    SchemaInterpretation.SalesforceAccount, compressed);
            ResponseEntity<String> result = restTemplate.exchange(getRestAPIHostPort() + uri, HttpMethod.POST,
                    requestEntity, String.class);
            return JsonUtils.deserialize(result.getBody(), new TypeReference<ResponseDocument<SourceFile>>() {
            });
        } else {
            String filename = DateTime.now().getMillis() + ".csv";
            String uri = String.format("/pls/models/fileuploads?fileName=%s&schema=%s&compressed=%s", filename,
                    SchemaInterpretation.SalesforceAccount, compressed);
            ResponseEntity<String> result = restTemplate.exchange(getRestAPIHostPort() + uri, HttpMethod.POST,
                    requestEntity, String.class);
            return JsonUtils.deserialize(result.getBody(), new TypeReference<ResponseDocument<SourceFile>>() {
            });
        }
    }

    @Test(groups = "functional")
    public void uploadFile() throws Exception {
        switchToExternalUser();
        ResponseDocument<SourceFile> response = submitFile(false, PATH, false);
        assertTrue(response.isSuccess());
        SourceFile fileResponse = new ObjectMapper().convertValue(response.getResult(), SourceFile.class);
        String contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, fileResponse.getPath());
        String expectedContents = FileUtils.readFileToString(new File(ClassLoader.getSystemResource(PATH).getPath()));
        assertEquals(contents, expectedContents);

        List<SourceFile> files = sourceFileEntityMgr.findAll();
        String path = fileResponse.getPath();
        foundTheFiles(path, files);
    }

    @Test(groups = "functional")
    public void uploadUnnamedFile() throws Exception {
        switchToExternalAdmin();
        ResponseDocument<SourceFile> response = submitFile(true, PATH, false);
        assertTrue(response.isSuccess());
        String path = response.getResult().getPath();
        String contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, path);
        String expectedContents = FileUtils.readFileToString(new File(ClassLoader.getSystemResource(PATH).getPath()));
        assertEquals(contents, expectedContents);

        List<SourceFile> files = sourceFileEntityMgr.findAll();
        foundTheFiles(path, files);
    }

    @Test(groups = "functional")
    public void uploadCompressedFile() throws Exception {
        switchToExternalAdmin();
        ResponseDocument<SourceFile> response = submitFile(false, COMPRESSED_PATH, true);
        assertTrue(response.isSuccess());
        String path = response.getResult().getPath();
        String contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, path);
        String expectedContents = ZipUtils.decompressFileToString(ClassLoader.getSystemResource(COMPRESSED_PATH)
                .getPath());
        assertEquals(contents, expectedContents);

        List<SourceFile> files = sourceFileEntityMgr.findAll();
        foundTheFiles(path, files);
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
