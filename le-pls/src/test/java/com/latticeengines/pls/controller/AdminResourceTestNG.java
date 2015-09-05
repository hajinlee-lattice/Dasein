package com.latticeengines.pls.controller;

import static org.testng.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.commons.httpclient.URIException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.pls.UserUpdateData;
import com.latticeengines.domain.exposed.security.*;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;

public class AdminResourceTestNG extends PlsFunctionalTestNGBase {

    private Tenant tenant;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private UserService userService;

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        deleteUserWithUsername("ron@lattice-engines.com");
        setupDbWithMarketoSMB("T1", "T1");
        tenant = tenantService.findByTenantId("T1");
    }

    @AfterClass(groups = "functional")
    public void teardown() {
        tenantService.discardTenant(tenant);
    }

    @Test(groups = "functional")
    public void addTenantWithProperMagicAuthenticationHeader() {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        Boolean result = restTemplate.postForObject(getRestAPIHostPort() + "/pls/admin/tenants", tenant, Boolean.class,
                new HashMap<>());
        assertTrue(result);

        Tenant t = tenantEntityMgr.findByTenantId("T1");
        assertNotNull(t);
    }

    @Test(groups = "functional")
    public void addTenantWithoutProperMagicAuthenticationHeader() {
        addMagicAuthHeader.setAuthValue("xyz");
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());

        boolean exception = false;
        try {
            restTemplate.postForObject(getRestAPIHostPort() + "/pls/admin/tenants", tenant, Boolean.class,
                    new HashMap<>());
        } catch (Exception e) {
            exception = true;
            String code = e.getMessage();
            assertEquals(code, "401");
        }
        assertTrue(exception);
    }

    @Test(groups = "functional")
    public void addExistingTenant() {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        Boolean result = restTemplate.postForObject(getRestAPIHostPort() + "/pls/admin/tenants", tenant, Boolean.class,
                new HashMap<>());
        assertTrue(result);

        tenant.setName("new name");
        result = restTemplate.postForObject(getRestAPIHostPort() + "/pls/admin/tenants", tenant, Boolean.class,
                new HashMap<>());
        assertTrue(result);
        Tenant t = tenantEntityMgr.findByTenantId("T1");
        assertNotNull(t);
        assertEquals(t.getName(), "new name");
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "functional", dependsOnMethods = { "addTenantWithProperMagicAuthenticationHeader" })
    public void getTenantsWithProperMagicAuthenticationHeader() {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        List<Tenant> tenants = restTemplate.getForObject(getRestAPIHostPort() + "/pls/admin/tenants", List.class);
        assertTrue(tenants.size() >= 1);
    }

    @Test(groups = "functional")
    public void getTenantsWithoutProperMagicAuthenticationHeader() {
        addMagicAuthHeader.setAuthValue("xyz");
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());

        boolean exception = false;
        try {
            restTemplate.getForObject(getRestAPIHostPort() + "/pls/admin/tenants", List.class);
        } catch (Exception e) {
            exception = true;
            String code = e.getMessage();
            assertEquals(code, "401");
        }
        assertTrue(exception);
    }

    @Test(groups = "functional", dependsOnMethods = { "addAdminUserBadArgs" })
    public void addAdminUser() {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        UserRegistrationWithTenant userRegistrationWithTenant = new UserRegistrationWithTenant();
        userRegistrationWithTenant.setTenant("T1");
        UserRegistration userRegistration = new UserRegistration();
        userRegistrationWithTenant.setUserRegistration(userRegistration);
        userRegistration.setUser(getUser());
        userRegistration.setCredentials(getCreds());

        Boolean result = restTemplate.postForObject(getRestAPIHostPort() + "/pls/admin/users",
                userRegistrationWithTenant, Boolean.class);
        assertTrue(result);

        assertNotNull(globalUserManagementService.getUserByEmail("ron@lattice-engines.com"));
    }

    @Test(groups = "functional", dataProvider = "userRegistrationDataProviderBadArgs")
    public void addAdminUserBadArgs(UserRegistrationWithTenant userRegistrationWithTenant) {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));

        Boolean result = restTemplate.postForObject(getRestAPIHostPort() + "/pls/admin/users",
                userRegistrationWithTenant, Boolean.class);
        assertFalse(result);
        assertNull(globalUserManagementService.getUserByEmail("ron@lattice-engines.com"));
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "functional")
    public void updateUserAccessLevels() throws URIException {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));

        String username = "tester_" + UUID.randomUUID().toString() + "@test.lattice.local";
        deleteUserWithUsername(username);
        createUser(username, username, "Test", "Tester");
        userService.assignAccessLevel(AccessLevel.EXTERNAL_USER, tenant.getId(), username);

        UserUpdateData data = new UserUpdateData();
        data.setAccessLevel(AccessLevel.INTERNAL_ADMIN.name());

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>(data.toString(), headers);

        ResponseEntity<ResponseDocument> responseEntity = restTemplate.exchange(getRestAPIHostPort()
                + "/pls/admin/users?tenant=" + tenant.getId() + "&username=" + username, HttpMethod.PUT, requestEntity,
                ResponseDocument.class);
        ResponseDocument response = responseEntity.getBody();
        Assert.assertTrue(response.isSuccess());

        AccessLevel accessLevel = userService.getAccessLevel(tenant.getId(), username);
        Assert.assertEquals(accessLevel, AccessLevel.INTERNAL_ADMIN);
        deleteUserWithUsername(username);
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "functional")
    public void updateUserAccessLevelsWrongArgs() throws URIException {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));

        String username = "tester_" + UUID.randomUUID().toString() + "@test.lattice.local";
        deleteUserWithUsername(username);
        createUser(username, username, "Test", "Tester");
        userService.assignAccessLevel(AccessLevel.EXTERNAL_USER, tenant.getId(), username);

        UserUpdateData data = new UserUpdateData();
        data.setAccessLevel(AccessLevel.INTERNAL_ADMIN.name());

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>(data.toString(), headers);

        ResponseEntity<ResponseDocument> responseEntity = restTemplate.exchange(getRestAPIHostPort()
                + "/pls/admin/users?tenant=" + tenant.getId() + "&username=nope", HttpMethod.PUT, requestEntity,
                ResponseDocument.class);
        ResponseDocument response = responseEntity.getBody();
        Assert.assertFalse(response.isSuccess(), "Update user with wrong username should fail.");

        responseEntity = restTemplate.exchange(getRestAPIHostPort() + "/pls/admin/users?tenant=nope&username="
                + username, HttpMethod.PUT, requestEntity, ResponseDocument.class);
        response = responseEntity.getBody();
        Assert.assertFalse(response.isSuccess(), "Update user with wrong tenant should fail.");

        deleteUserWithUsername(username);
    }

    @DataProvider(name = "userRegistrationDataProviderBadArgs")
    public static Object[][] userRegistrationDataProviderBadArgs() {
        User user = getUser();
        Credentials creds = getCreds();

        // No user registration
        UserRegistrationWithTenant urwt1 = new UserRegistrationWithTenant();
        urwt1.setTenant("T1");

        // No tenant
        UserRegistrationWithTenant urwt2 = new UserRegistrationWithTenant();
        UserRegistration ur2 = new UserRegistration();
        urwt2.setUserRegistration(ur2);
        ur2.setUser(user);
        ur2.setCredentials(creds);

        // With tenant and user registration, but user registration has no user
        UserRegistrationWithTenant urwt3 = new UserRegistrationWithTenant();
        UserRegistration ur3 = new UserRegistration();
        urwt3.setUserRegistration(ur3);
        ur3.setCredentials(creds);

        // With tenant and user registration, but user registration has no
        // credentials
        UserRegistrationWithTenant urwt4 = new UserRegistrationWithTenant();
        UserRegistration ur4 = new UserRegistration();
        urwt4.setUserRegistration(ur4);
        ur4.setUser(user);

        return new Object[][] { { urwt1 }, //
                { urwt2 }, //
                { urwt3 }, //
                { urwt4 } };
    }

    static User getUser() {
        User user = new User();
        user.setActive(true);
        user.setEmail("ron@lattice-engines.com");
        user.setFirstName("Ron");
        user.setLastName("Gonzalez");
        user.setUsername("ron@lattice-engines.com");
        return user;
    }

    static Credentials getCreds() {
        Credentials creds = new Credentials();
        creds.setUsername("ron@lattice-engines.com");
        creds.setPassword(generalPasswordHash);
        return creds;

    }

}
