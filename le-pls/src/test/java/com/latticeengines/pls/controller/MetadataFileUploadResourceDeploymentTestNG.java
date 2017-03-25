package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class MetadataFileUploadResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String PATH = "com/latticeengines/pls/service/impl/metadatafileuploadserviceimpl";

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private MetadataProxy metadataProxy;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
    }

    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, String.format("/Pods/Default/Contracts/%s", mainTestTenant.getName()));
    }

    @Test(groups = "deployment", dependsOnMethods = { "uploadFile" })
    public void getModules() {
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "deployment")
    public void uploadFile() {
        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        map.add("metadataFile", new ClassPathResource(PATH + "/rfpmml.xml"));
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<LinkedMultiValueMap<String, Object>>(
                map, headers);
        String path = String.format("/pls/metadatauploads/modules/module1/%s?artifactName=abc",
                ArtifactType.PMML.getUrlToken());
        ResponseEntity<ResponseDocument> result = restTemplate.exchange(getRestAPIHostPort() + path, HttpMethod.POST,
                requestEntity, ResponseDocument.class);
        assertTrue(((String) result.getBody().getResult()).endsWith("/Metadata/module1/PMMLFiles/abc.xml"));

        assertEquals(metadataProxy.getModule(mainTestTenant.getId(), "module1").getArtifacts().size(), 1);
    }

    @Test(groups = "deployment")
    public void uploadMalformedPivotFile() {
        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        map.add("metadataFile", new ClassPathResource(PATH + "/pivot.csv"));
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<LinkedMultiValueMap<String, Object>>(
                map, headers);
        String path = String.format("/pls/metadatauploads/modules/module1/%s?artifactName=abcd",
                ArtifactType.PivotMapping.getUrlToken());
        try {
            restTemplate.exchange(getRestAPIHostPort() + path, HttpMethod.POST, requestEntity, String.class);
            assertTrue(false);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(
                            "Unable to find required columns [SourceColumn, TargetColumn, SourceColumnType]"));
        }
    }
}
