package com.latticeengines.upgrade.pls;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.upgrade.functionalframework.UpgradeFunctionalTestNGBase;

public class PlsGaManagerTestNG extends UpgradeFunctionalTestNGBase {

    private static final String PLS_USRNAME = "bnguyen@lattice-engines.com";
    private static final String PLS_PASSWORD = "tahoe";

    @Autowired
    private PlsGaManager plsGaManager;

    @Value("${upgrade.pls.url}")
    private String plsApiHost;

    @BeforeClass(groups = "functional")
    public void setup() { tearDown(); }

    @AfterClass(groups = "functional")
    public void tearDown() {
        String url = plsApiHost + "/pls/admin/tenants/" + TUPLE_ID;
        magicRestTemplate.delete(url);
        Assert.assertFalse(tenantExists(), "Test tenant should be deleted.");
    }

    @Test(groups = "functional")
    public void testRegisterTenant() {
        plsGaManager.registerTenant(CUSTOMER);
        Assert.assertTrue(tenantExists(), "Test tenant does not exists.");
    }

    @Test(groups = "functional", dependsOnMethods = "testRegisterTenant")
    public void testAssignSuperUsers() throws IOException {
        plsGaManager.setupAdminUsers(CUSTOMER);

        List<BasicNameValuePair> headers = new ArrayList<>();
        headers.add(new BasicNameValuePair("Content-Type", "application/json; charset=utf-8"));
        headers.add(new BasicNameValuePair("Accept", "application/json"));

        String url = plsApiHost + "/pls/login";
        String payload = "{\"Username\":\"" + PLS_USRNAME + "\", \"Password\":\""
                + DigestUtils.sha256Hex(PLS_PASSWORD) + "\"}";
        String response = HttpClientWithOptionalRetryUtils.sendPostRequest(url, false, headers, payload);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readTree(response);

        boolean seeTestTenant = false;
        for (JsonNode tenant: json.get("Result").get("Tenants")) {
            if (TUPLE_ID.equals(tenant.get("Identifier").asText())) {
                seeTestTenant = true;
                break;
            }
        }
        Assert.assertTrue(seeTestTenant, "Did not see the test tenant in the tenent list upon login PLS.");
    }

    @Test(groups = "functional", dependsOnMethods = "testAssignSuperUsers")
    public void testSetModelActivity() throws IOException {
        uploadModel();

        plsGaManager.setModelActivity(MODEL_GUID, true);

        List response = restTemplate.getForObject(plsApiHost + "/pls/modelsummaries/", List.class);
        ModelSummary summary = JsonUtils.deserialize(JsonUtils.serialize(response.get(0)), ModelSummary.class);
        Assert.assertEquals(summary.getStatus(), ModelSummaryStatus.ACTIVE);
    }

    @Test(groups = "functional", dependsOnMethods = "testSetModelActivity")
    public void testGetModelIDs() {
        List<String> ids = plsGaManager.getModelIds(CUSTOMER);
        Assert.assertTrue(ids.contains(MODEL_GUID), "Should contain the testing modelID.");
    }

    private boolean tenantExists() {
        String url = plsApiHost + "/pls/admin/tenants";
        ArrayNode tenants = magicRestTemplate.getForObject(url, ArrayNode.class);
        for (JsonNode tenant: tenants) {
            if (TUPLE_ID.equals(tenant.get("Identifier").asText())) return true;
        }
        return false;
    }

    private void uploadModel() throws IOException {
        InputStream ins = getClass().getClassLoader().getResourceAsStream("modelsummary.json");
        assertNotNull(ins, "Testing json file is missing");

        ModelSummary data = new ModelSummary();
        Tenant fakeTenant = new Tenant();
        fakeTenant.setId("FAKE_TENANT");
        fakeTenant.setName("Fake Tenant");
        fakeTenant.setPid(-1L);
        data.setTenant(fakeTenant);
        data.setRawFile(new String(IOUtils.toByteArray(ins)));

        Tenant tenant = new Tenant();
        tenant.setId(TUPLE_ID);
        tenant.setName(CUSTOMER);
        loginAndAttach(PLS_USRNAME, PLS_PASSWORD, tenant);

        ModelSummary newSummary = restTemplate.postForObject(
                plsApiHost + "/pls/modelsummaries?raw=true", data, ModelSummary.class);
        assertNotNull(newSummary);
        List response = restTemplate.getForObject(plsApiHost + "/pls/modelsummaries/", List.class);
        assertNotNull(response);
        assertEquals(response.size(), 1);
    }

    private UserDocument loginAndAttach(String username, String password, Tenant tenant) {
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(DigestUtils.sha256Hex(password));

        LoginDocument doc = restTemplate.postForObject(plsApiHost + "/pls/login", creds,
                LoginDocument.class);

        addAuthHeader.setAuthValue(doc.getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addAuthHeader}));

        return restTemplate.postForObject(plsApiHost + "/pls/attach", tenant, UserDocument.class);
    }
}
