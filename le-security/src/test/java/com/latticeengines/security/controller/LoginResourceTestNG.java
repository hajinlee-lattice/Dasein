package com.latticeengines.security.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.pls.UserUpdateData;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.globalauth.GlobalTenantManagementService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;

public class LoginResourceTestNG extends SecurityFunctionalTestNGBase {

    @Autowired
    GlobalTenantManagementService globalTenantManagementService;

    @Autowired
    TenantService tenantService;

    @Autowired
    private UserService userService;

    @BeforeClass(groups = { "functional", "deployment" })
    public void setup() throws Exception {
        try {
            login(adminUsername, adminPassword);
        } catch (Exception e) {
            createAdminUser();
        }
    }

    @Test(groups = { "functional", "deployment" })
    public void login() {
        Credentials creds = new Credentials();
        creds.setUsername(adminUsername);
        creds.setPassword(DigestUtils.sha256Hex(adminPassword));

        LoginDocument loginDoc = restTemplate.postForObject(getRestAPIHostPort() + "/login", creds,
                LoginDocument.class);
        assertNotNull(loginDoc.getData());
    }

    @Test(groups = { "functional", "deployment" })
    public void loginBadPassword() {
        Credentials creds = new Credentials();
        creds.setUsername(adminUsername);
        creds.setPassword("badpassword");

        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());

        try {
            restTemplate.postForObject(getRestAPIHostPort() + "/login", creds, Session.class);
        } catch (Exception e) {
            String code = e.getMessage();
            assertEquals(code, "401");
        }
    }

    @Test(groups = { "functional", "deployment" })
    public void tenantsOutOfSync() {
        Tenant tenant = new Tenant();
        tenant.setName("LoginResource Test Tenant");
        tenant.setId("LoginResource_Test_Tenant");

        try {
            // Simulate tenant registered in GA but not in LP DB.
            globalTenantManagementService.registerTenant(tenant);
            userService.assignAccessLevel(AccessLevel.SUPER_ADMIN, tenant.getId(), adminUsername);

            Credentials creds = new Credentials();
            creds.setUsername(adminUsername);
            creds.setPassword(DigestUtils.sha256Hex(adminPassword));

            LoginDocument loginDoc = restTemplate.postForObject(getRestAPIHostPort() + "/login", creds,
                    LoginDocument.class);
            assertFalse(loginDoc.getResult().getTenants().contains(tenant));
        } finally {
            userService.deleteUser(tenant.getId(), adminUsername);
            globalTenantManagementService.discardTenant(tenant);
        }
    }

    @Test(groups = { "functional", "deployment" })
    public void logout() {
        Credentials creds = new Credentials();
        creds.setUsername(adminUsername);
        creds.setPassword(DigestUtils.sha256Hex(adminPassword));

        // Do we need to log in before we log out?  I'm not sure if I'm doing this correctly.
        LoginDocument loginDoc = restTemplate.postForObject(getRestAPIHostPort() + "/login", creds,
                LoginDocument.class);
        assertNotNull(loginDoc.getData());

        // Extract authorization token from login response to use for logout.
        String token = loginDoc.getUniqueness() + "." + loginDoc.getRandomness();

        // Build HTTP header that includes the authorization token.
        HttpHeaders headers = new HttpHeaders();
        headers.set(Constants.AUTHORIZATION, token);
        HttpEntity<HttpHeaders> headerEntity = new HttpEntity<>(headers);

        HttpEntity<SimpleBooleanResponse> responseEntity = restTemplate.exchange(
                getRestAPIHostPort() + "/logout", HttpMethod.GET, headerEntity, SimpleBooleanResponse.class);
        assertTrue(responseEntity.getBody().isSuccess());
    }

    @Test(groups = { "functional", "deployment" })
    public void logoutEmptyToken() {
        Credentials creds = new Credentials();
        creds.setUsername(adminUsername);
        creds.setPassword(DigestUtils.sha256Hex(adminPassword));

        // Do we need to log in before we log out?  I'm not sure if I'm doing this correctly.
        LoginDocument loginDoc = restTemplate.postForObject(getRestAPIHostPort() + "/login", creds,
                LoginDocument.class);
        assertNotNull(loginDoc.getData());

        // Set an empty authorization token in the logout request HTTP header.
        HttpHeaders headers = new HttpHeaders();
        headers.set(Constants.AUTHORIZATION, "");
        HttpEntity<HttpHeaders> headerEntity = new HttpEntity<>(headers);

        HttpEntity<SimpleBooleanResponse> responseEntity = restTemplate.exchange(
                getRestAPIHostPort() + "/logout", HttpMethod.GET, headerEntity, SimpleBooleanResponse.class);
        assertTrue(responseEntity.getBody().isSuccess());
    }

    @Test(groups = { "functional", "deployment" })
    public void logoutInvalidToken() {
        Credentials creds = new Credentials();
        creds.setUsername(adminUsername);
        creds.setPassword(DigestUtils.sha256Hex(adminPassword));

        // Do we need to log in before we log out?  I'm not sure if I'm doing this correctly.
        LoginDocument loginDoc = restTemplate.postForObject(getRestAPIHostPort() + "/login", creds,
                LoginDocument.class);
        assertNotNull(loginDoc.getData());

        // Extract authorization token from login response to use for logout HTTP header.  Do not place a period
        // between the uniqueness and randomness component to trigger an invalid token error.
        String token = loginDoc.getUniqueness() + loginDoc.getRandomness();
        HttpHeaders headers = new HttpHeaders();
        headers.set(Constants.AUTHORIZATION, token);
        HttpEntity<HttpHeaders> headerEntity = new HttpEntity<>(headers);

        HttpEntity<SimpleBooleanResponse> responseEntity = restTemplate.exchange(
                getRestAPIHostPort() + "/logout", HttpMethod.GET, headerEntity, SimpleBooleanResponse.class);
        assertTrue(responseEntity.getBody().isSuccess());
    }

    @Test(groups = { "functional", "deployment" })
    public void loginWithExpiredTenant() {

        UserDocument document = loginAndAttach(adminUsername, adminPassword, getAdminTenant());
        // set expiration date for admin user on tenant
        UserUpdateData data = new UserUpdateData();
        data.setAccessLevel(AccessLevel.SUPER_ADMIN.toString());
        data.setExpirationDate(System.currentTimeMillis());
        HttpHeaders headers = new HttpHeaders();
        String token = document.getTicket().getUniqueness() + "." + document.getTicket().getRandomness();
        headers.set(Constants.AUTHORIZATION, token);
        HttpEntity<UserUpdateData> entity = new HttpEntity<>(data, headers);

        // update expiration date to current time
        HttpEntity<SimpleBooleanResponse> responseEntity = restTemplate.exchange(
                getRestAPIHostPort() + String.format("/users/%s", JsonUtils.serialize(adminUsername)), HttpMethod.PUT,
                entity,
                SimpleBooleanResponse.class);
        assertTrue(responseEntity.getBody().isSuccess());

        HttpEntity<HttpHeaders> headerEntity = new HttpEntity<>(headers);
        HttpEntity<SimpleBooleanResponse> responseEntity1 = restTemplate.exchange(getRestAPIHostPort() + "/logout",
                HttpMethod.GET, headerEntity,
                SimpleBooleanResponse.class);
        assertTrue(responseEntity1.getBody().isSuccess());

        // login again, verify that the tenant can't visit
        Credentials creds = new Credentials();
        creds.setUsername(adminUsername);
        creds.setPassword(DigestUtils.sha256Hex(adminPassword));
        LoginDocument loginDoc = restTemplate.postForObject(getRestAPIHostPort() + "/login", creds,
                LoginDocument.class);
        assertNotNull(loginDoc.getResult());
        List<Tenant> validatedTenants = loginDoc.getResult().getTenants();
        System.out.println("test " + JsonUtils.serialize(validatedTenants));
        Assert.assertTrue(CollectionUtils.isEmpty(validatedTenants));
    }
}
