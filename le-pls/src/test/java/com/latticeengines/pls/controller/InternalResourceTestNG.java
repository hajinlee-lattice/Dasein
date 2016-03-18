package com.latticeengines.pls.controller;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.service.TenantService;

@Component("internalResourceTestNG")
public class InternalResourceTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private TenantService tenantService;

    private Tenant tenant;

    @BeforeClass(groups = { "functional", "deployment" })
    public void setup() throws Exception {
        setUpMarketoEloquaTestEnvironment();

        tenant = new Tenant();
        tenant.setName("Internal Resource Test Tenant");
        tenant.setId("INTERNAL_RESOURCE_TEST_TENANT");
        tenantService.discardTenant(tenant);
        tenantService.registerTenant(tenant);
    }

    @AfterClass(groups = { "functional", "deployment" })
    public void teardown() throws Exception {
        tenantService.discardTenant(tenant);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test(groups = "functional")
    public void updateModelSummary() {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));

        List<ModelSummary> modelSummaries = modelSummaryEntityMgr.getAll();
        AttributeMap attrMap = new AttributeMap();
        attrMap.put("Status", "UpdateAsActive");

        String restAPIHostPort = getRestAPIHostPort();
        for (ModelSummary modelSummary : modelSummaries) {
            String url = String.format("%s/pls/internal/modelsummaries/%s", restAPIHostPort, modelSummary.getId());
            HttpEntity<AttributeMap> requestEntity = new HttpEntity<>(attrMap);
            ResponseEntity<ResponseDocument> response = restTemplate.exchange(url, HttpMethod.PUT, requestEntity,
                    ResponseDocument.class);
            ResponseDocument responseDoc = response.getBody();
            Assert.assertTrue(responseDoc.isSuccess());
            Map<String, Object> result = (Map) response.getBody().getResult();
            Assert.assertTrue((boolean) result.get("Exists"));
        }

        modelSummaries = modelSummaryEntityMgr.getAll();

        for (ModelSummary modelSummary : modelSummaries) {
            Assert.assertEquals(modelSummary.getStatus(), ModelSummaryStatus.ACTIVE);
        }

    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test(groups = "functional")
    public void updateupdateModelSummaryNotExists() {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));

        AttributeMap attrMap = new AttributeMap();
        attrMap.put("Status", "UpdateAsActive");
        String restAPIHostPort = getRestAPIHostPort();
        String url = String.format("%s/pls/internal/modelsummaries/%s", restAPIHostPort, "xyz");
        HttpEntity<AttributeMap> requestEntity = new HttpEntity<>(attrMap);
        ResponseEntity<ResponseDocument> response = restTemplate.exchange(url, HttpMethod.PUT, requestEntity,
                ResponseDocument.class);
        ResponseDocument responseDoc = response.getBody();
        Assert.assertFalse(responseDoc.isSuccess());
        Map<String, Object> result = (Map) response.getBody().getResult();
        Assert.assertFalse((boolean) result.get("Exists"));
    }

}
