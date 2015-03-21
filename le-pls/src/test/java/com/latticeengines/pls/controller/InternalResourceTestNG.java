package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.util.URIUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.domain.exposed.api.Status;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.ResponseDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.globalauth.authentication.GlobalAuthenticationService;
import com.latticeengines.pls.globalauth.authentication.GlobalTenantManagementService;
import com.latticeengines.pls.globalauth.authentication.GlobalUserManagementService;
import com.latticeengines.pls.globalauth.authentication.impl.Constants;

public class InternalResourceTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @Autowired
    private GlobalTenantManagementService globalTenantManagementService;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @BeforeClass(groups = { "functional", "deployment" })
    public void setup() throws Exception {
        Ticket ticket = globalAuthenticationService.authenticateUser("admin", DigestUtils.sha256Hex("admin"));
        String tenant1 = ticket.getTenants().get(0).getId();
        String tenant2 = ticket.getTenants().get(1).getId();
        setupDb(tenant1, tenant2);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test(groups = "functional")
    public void update() throws Exception {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));

        List<ModelSummary> modelSummaries = modelSummaryEntityMgr.getAll();
        AttributeMap attrMap = new AttributeMap();
        attrMap.put("Status", "UpdateAsActive");

        String restAPIHostPort = getRestAPIHostPort();
        for (ModelSummary modelSummary : modelSummaries) {
            String url = String.format("%s/pls/internal/modelsummaries/%s", restAPIHostPort, modelSummary.getId());
            HttpEntity<AttributeMap> requestEntity = new HttpEntity<AttributeMap>(attrMap);
            ResponseEntity<ResponseDocument> response = restTemplate.exchange(url, HttpMethod.PUT, requestEntity,
                    ResponseDocument.class);
            ResponseDocument responseDoc = response.getBody();
            assertTrue(responseDoc.isSuccess());
            Map<String, Object> result = (Map) ((ResponseDocument) response.getBody()).getResult();
            assertTrue((boolean) result.get("Exists"));
        }

        modelSummaries = modelSummaryEntityMgr.getAll();

        for (ModelSummary modelSummary : modelSummaries) {
            assertEquals(modelSummary.getStatus(), ModelSummaryStatus.ACTIVE);
        }

    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test(groups = "functional")
    public void updateNotExists() throws Exception {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));

        AttributeMap attrMap = new AttributeMap();
        attrMap.put("Status", "UpdateAsActive");
        String restAPIHostPort = getRestAPIHostPort();
        String url = String.format("%s/pls/internal/modelsummaries/%s", restAPIHostPort, "xyz");
        HttpEntity<AttributeMap> requestEntity = new HttpEntity<AttributeMap>(attrMap);
        ResponseEntity<ResponseDocument> response = restTemplate.exchange(url, HttpMethod.PUT, requestEntity,
                ResponseDocument.class);
        ResponseDocument responseDoc = response.getBody();
        assertFalse(responseDoc.isSuccess());
        Map<String, Object> result = (Map) ((ResponseDocument) response.getBody()).getResult();
        assertFalse((boolean) result.get("Exists"));
    }

    @Test(groups = "functional")
    public void calculate() {
        String restAPIHostPort = getRestAPIHostPort();
        String url = String.format("%s/pls/internal/add/1/2", restAPIHostPort);
        Status status = restTemplate.getForObject(url, Status.class);
        assertNotNull(status);
    }

    @Test(groups = "functional")
    public void deleteTestingUsers() throws URIException {
        Tenant tenant = new Tenant();
        tenant.setName("Test Tenant");
        tenant.setId("Test_" + UUID.randomUUID().toString());
        globalTenantManagementService.registerTenant(tenant);

        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));

        for (int i = 0; i < 10; i++) {
            String username = "tester_" + UUID.randomUUID().toString() + "@test.com";
            assertTrue(globalUserManagementService.deleteUser(username));
            createUser(username, username, "Test", "Tester");
            grantDefaultRights(tenant.getId(), username);
        }

        String pattern = URIUtil.encodeQuery("tester_[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}@test.com");

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        HttpEntity<JsonNode> requestEntity = new HttpEntity<>(null, headers);
        ResponseEntity<ResponseDocument> responseEntity = restTemplate.exchange(
            getRestAPIHostPort() + "/pls/internal/users?namepattern=" + pattern,
            HttpMethod.DELETE,
            requestEntity,
            ResponseDocument.class
        );
        ResponseDocument response = responseEntity.getBody();
        assertFalse(response.isSuccess());

        responseEntity = restTemplate.exchange(
            getRestAPIHostPort() + "/pls/internal/users?namepattern=" + pattern + "&tenant=" + tenant.getId(),
            HttpMethod.DELETE,
            requestEntity,
            ResponseDocument.class
        );
        response = responseEntity.getBody();
        assertTrue(response.isSuccess());

        List<AbstractMap.SimpleEntry<User, List<String>>> userRightsList = globalUserManagementService.getAllUsersOfTenant(tenant.getId());
        boolean cleaned = true;
        for (AbstractMap.SimpleEntry<User, List<String>> userRight: userRightsList) {
            User user = userRight.getKey();
            if (user.getUsername().matches(pattern)) {
                cleaned = false;
                break;
            }
        }
        assertTrue(cleaned);

        globalTenantManagementService.discardTenant(tenant);
    }
}
