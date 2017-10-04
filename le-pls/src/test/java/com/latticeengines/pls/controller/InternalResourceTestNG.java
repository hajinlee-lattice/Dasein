package com.latticeengines.pls.controller;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.service.TenantService;

@Component("internalResourceTestNG")
public class InternalResourceTestNG extends PlsFunctionalTestNGBaseDeprecated {

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private TenantService tenantService;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    private Tenant tenant;

    @BeforeClass(groups = { "functional", "deployment" })
    public void setup() throws Exception {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
        setupMarketoEloquaTestEnvironment();

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

    @Test(groups = "functional")
    public void updateModelSummary() {
        List<ModelSummary> modelSummaries = modelSummaryEntityMgr.findAllValid();
        for (ModelSummary modelSummary : modelSummaries) {
            internalResourceRestApiProxy.activateModelSummary(modelSummary.getId());
        }
        modelSummaries = modelSummaryEntityMgr.findAll();
        for (ModelSummary modelSummary : modelSummaries) {
            Assert.assertEquals(modelSummary.getStatus(), ModelSummaryStatus.ACTIVE);
        }

    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test(groups = "functional")
    public void updateModelSummaryNotExists() {
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

    @Test(groups = "functional")
    public void testGetModelSummariesModifiedWithinTimeFrame() {
        List<ModelSummary> modelSummaries = internalResourceRestApiProxy
                .getModelSummariesModifiedWithinTimeFrame(120000L);
        Assert.assertNotNull(modelSummaries);
        int size = modelSummaries.size();
        Assert.assertTrue(size >= 2);
    }
}
