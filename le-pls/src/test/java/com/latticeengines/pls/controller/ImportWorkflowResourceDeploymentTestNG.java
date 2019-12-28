package com.latticeengines.pls.controller;

import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;
import com.latticeengines.domain.exposed.util.ImportWorkflowSpecUtils;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.core.ImportWorkflowSpecProxy;

public class ImportWorkflowResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private String systemType = "fakeType1";
    private String systemObject = "fakeObject1";

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        MultiTenantContext.setTenant(mainTestTenant);
    }

    @Inject
    private ImportWorkflowSpecProxy importWorkflowSpecProxy;

    @Test(groups = "deployment")
    public void testUploadSpec() throws Exception {
        // test upload
        ImportWorkflowSpec importWorkflowSpec = new ImportWorkflowSpec();
        importWorkflowSpec.setSystemName(this.getClass().getSimpleName());
        importWorkflowSpec.setSystemType(systemType);
        importWorkflowSpec.setSystemObject(systemObject);
        String tenantId = MultiTenantContext.getShortTenantId();
        String url = getRestAPIHostPort() + String.format("/pls/importworkflow/specs/upload?systemType=%s&systemObject=%s",
                systemType, systemObject);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);
        MultiValueMap<String, Object> parts = new LinkedMultiValueMap<>();
        Resource source = new ByteArrayResource(JsonUtils.serialize(importWorkflowSpec).getBytes()) {
            @Override
            public String getFilename() {
                return ImportWorkflowSpecUtils.constructSpecName(systemType, systemObject);
            }
        };
        parts.add("file", source);
        HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(parts,
                headers);
        ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST,
                requestEntity, String.class);

        // test download
        String downloadURL = getRestAPIHostPort() + String.format("/pls/importworkflow/specs/download?systemType=%s&systemObject=%s",
                systemType, systemObject);
        headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(MediaType.ALL));
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<byte[]> downloadResponse = restTemplate.exchange(downloadURL, HttpMethod.GET, entity,
                byte[].class);
        String content = downloadResponse.getHeaders().getFirst("Content-Disposition");
        Assert.assertTrue(content.contains(ImportWorkflowSpecUtils.constructSpecName(systemType, systemObject)));
        Assert.assertNotNull(response.getBody());

        // test get and delete
        ImportWorkflowSpec importSpec = importWorkflowSpecProxy.getImportWorkflowSpec(tenantId, systemType,
                systemObject);
        Assert.assertNotNull(importSpec);
        importWorkflowSpecProxy.deleteSpecFromS3(tenantId, systemType, systemObject);
        importSpec = importWorkflowSpecProxy.getImportWorkflowSpec(tenantId, systemType, systemObject);

        Assert.assertNull(importSpec);
    }

    @Test(groups = "deployment", dependsOnMethods = "testUploadSpec")
    public void testListSpec() throws Exception {
        // set type and object empty, will get all spec in S3
        String url = getRestAPIHostPort() + "/pls/importworkflow/specs/list";
        List<?> specList = restTemplate.getForObject(url, List.class);
        Assert.assertTrue(CollectionUtils.isNotEmpty(specList));
        Assert.assertTrue(specList.size() > 0);
        url = getRestAPIHostPort() + String.format("/pls/importworkflow/specs/list?systemType=%s&systemObject=%s",
                systemType, systemObject);
        specList = restTemplate.getForObject(url, List.class);
        Assert.assertTrue(CollectionUtils.isEmpty(specList));
    }

    @Test(groups = "deployment", dependsOnMethods = "testListSpec")
    public void testOperationByExternalUser() throws Exception {
        switchToExternalUser();
        String url = getRestAPIHostPort() + "/pls/importworkflow/specs/list";
        try {
            List<?> specList = restTemplate.getForObject(url, List.class);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "403 FORBIDDEN: ");
        }

    }
}
