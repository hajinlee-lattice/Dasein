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

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.api.Status;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.CrmConstants;
import com.latticeengines.pls.service.CrmCredentialService;
import com.latticeengines.pls.service.TenantConfigService;
import com.latticeengines.pls.service.TenantService;
import com.latticeengines.security.exposed.Constants;

@Component("internalResourceTestNG")
public class InternalResourceTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private CrmCredentialService crmCredentialService;

    @Autowired
    private InternalResource internalResource;

    @Autowired
    private TenantConfigService tenantConfigService;

    private Tenant tenant;

    @Value("${pls.api.hostport}")
    private String hostPort;

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

    @Test(groups = "functional")
    public void calculate() {
        String restAPIHostPort = getRestAPIHostPort();
        String url = String.format("%s/pls/internal/add/1/2", restAPIHostPort);
        Status status = restTemplate.getForObject(url, Status.class);
        Assert.assertNotNull(status);
    }

    @Test(groups = "functional")
    public void cleanupTestTenant() throws Exception {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));

        // ==================================================
        // save CRM credentials
        // ==================================================
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName("apeters-widgettech@lattice-engines.com");
        crmCredential.setPassword("Happy2010");
        crmCredential.setSecurityToken("oIogZVEFGbL3n0qiAp6F66TC");
        String tenantId = mainTestingTenant.getId();
        try {
            CrmCredential newCrmCredential = crmCredentialService.verifyCredential(CrmConstants.CRM_SFDC, tenantId,
                    true, crmCredential);
            Assert.assertEquals(newCrmCredential.getOrgId(), "00D80000000KvZoEAK");
            Assert.assertEquals(newCrmCredential.getPassword(), "Happy2010");
        } catch (Exception e) {
            // ignore
        }

        // ==================================================
        // verify empty CRM credentials
        // ==================================================
        Camille camille = CamilleEnvironment.getCamille();
        CustomerSpace space = CustomerSpace.parse(tenantId);
        Path path = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), space.getContractId(),
                space.getTenantId(), space.getSpaceId());

        Path newPath = path.append(CrmConstants.CRM_SFDC).append("Production");
        Assert.assertFalse(camille.exists(newPath));
    }


}
