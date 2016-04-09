package com.latticeengines.pls.controller;

import static org.testng.Assert.assertTrue;

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
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;

public class MetadataFileUploadResourceTestNG extends PlsFunctionalTestNGBaseDeprecated {
    
    private static final String PATH = "com/latticeengines/pls/service/impl/metadatafileuploadserviceimpl/rfpmml.xml";
    
    @Autowired
    private Configuration yarnConfiguration;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, String.format("/Pods/Default/Contracts/%sPLSTenant1", contractId));
        setupMarketoEloquaTestEnvironment();
    }

    @Test(groups = "functional", dependsOnMethods = { "uploadFile" })
    public void getModules() {
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "functional")
    public void uploadFile() {
        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        map.add("metadataFile", new ClassPathResource(PATH));
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<LinkedMultiValueMap<String, Object>>(
                map, headers);
        String path = String.format("/pls/metadatauploads/modules/module1/%s?artifactName=abc", ArtifactType.PMML.getUrlToken());
        ResponseEntity<ResponseDocument> result = restTemplate.exchange(getRestAPIHostPort() + path, HttpMethod.POST,
                requestEntity, ResponseDocument.class);
        assertTrue(((String) result.getBody().getResult()).endsWith("/Metadata/module1/PMMLFiles/abc.xml"));
    }
}
