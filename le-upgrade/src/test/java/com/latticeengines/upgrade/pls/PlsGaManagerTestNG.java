package com.latticeengines.upgrade.pls;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.upgrade.functionalframework.UpgradeFunctionalTestNGBase;

public class PlsGaManagerTestNG extends UpgradeFunctionalTestNGBase {

    private static final String PLS_USRNAME = "bnguyen@lattice-engines.com";
    private static final String PLS_PASSWORD_HASH = "3ff74a580f8b39f039822455e92c2ef25658229622f16dc0f9918222c0be4900";

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
        String payload = "{\"Username\":\"" + PLS_USRNAME + "\", \"Password\":\"" + PLS_PASSWORD_HASH + "\"}";
        String response = HttpClientWithOptionalRetryUtils.sendPostRequest(url, false, headers, payload);
        System.out.println(response);

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

    private boolean tenantExists() {
        String url = plsApiHost + "/pls/admin/tenants";
        ArrayNode tenants = magicRestTemplate.getForObject(url, ArrayNode.class);
        for (JsonNode tenant: tenants) {
            if (TUPLE_ID.equals(tenant.get("Identifier").asText())) return true;
        }
        return false;
    }
}
