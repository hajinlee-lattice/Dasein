package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;
import com.latticeengines.pls.entitymanager.TenantEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.globalauth.authentication.GlobalUserManagementService;
import com.latticeengines.pls.globalauth.authentication.impl.Constants;

public class AdminResourceTestNG extends PlsFunctionalTestNGBase {

    private Tenant tenant;
    
    @Autowired
    private TenantEntityMgr tenantEntityMgr;
    
    @Autowired
    private GlobalUserManagementService globalUserManagementService;
    
    @BeforeClass(groups = { "functional", "deployment" })
    public void setup() {
        deleteUser("ron@lattice-engines.com");
        tenantEntityMgr.deleteAll();
        tenant = new Tenant();
        tenant.setId("T1");
        tenant.setName("T1");
    }
    
    @Test(groups = { "functional", "deployment" })
    public void addTenantWithProperMagicAuthenticationHeader() {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        Boolean result = restTemplate.postForObject(getRestAPIHostPort() + "/pls/admin/tenants", tenant, Boolean.class, new HashMap<>());
        assertTrue(result);
        
        Tenant t = tenantEntityMgr.findByTenantId("T1");
        assertNotNull(t);
    }

    @Test(groups = { "functional", "deployment" })
    public void addTenantWithoutProperMagicAuthenticationHeader() {
        addMagicAuthHeader.setAuthValue("xyz");
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());

        boolean exception = false;
        try {
            restTemplate.postForObject(getRestAPIHostPort() + "/pls/admin/tenants", tenant, Boolean.class, new HashMap<>());
        } catch (Exception e) {
            exception = true;
            String code = e.getMessage();
            assertEquals(code, "401");
        }
        assertTrue(exception);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = { "functional", "deployment" }, dependsOnMethods = { "addTenantWithProperMagicAuthenticationHeader" })
    public void getTenantsWithProperMagicAuthenticationHeader() {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        List<Tenant> tenants = restTemplate.getForObject(getRestAPIHostPort() + "/pls/admin/tenants", List.class);
        assertEquals(tenants.size(), 1);
    }

    @Test(groups = { "functional", "deployment" })
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
    
    @Test(groups = { "functional", "deployment" }, dependsOnMethods = { "addAdminUserBadArgs" })
    public void addAdminUser() {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        UserRegistrationWithTenant userRegistrationWithTenant = new UserRegistrationWithTenant();
        userRegistrationWithTenant.setTenant("T1");
        UserRegistration userRegistration = new UserRegistration();
        userRegistrationWithTenant.setUserRegistration(userRegistration);
        userRegistration.setUser(getUser());
        userRegistration.setCredentials(getCreds());
        
        Boolean result = restTemplate.postForObject(getRestAPIHostPort() + "/pls/admin/users", userRegistrationWithTenant, Boolean.class);
        assertTrue(result);
        
        assertNotNull(globalUserManagementService.getUserByEmail("ron@lattice-engines.com"));
    }

    @Test(groups = { "functional", "deployment" }, dataProvider = "userRegistrationDataProviderBadArgs")
    public void addAdminUserBadArgs(UserRegistrationWithTenant userRegistrationWithTenant) {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        
        Boolean result = restTemplate.postForObject(getRestAPIHostPort() + "/pls/admin/users", userRegistrationWithTenant, Boolean.class);
        assertFalse(result);
        assertNull(globalUserManagementService.getUserByEmail("ron@lattice-engines.com"));
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
        
        // With tenant and user registration, but user registration has no credentials
        UserRegistrationWithTenant urwt4 = new UserRegistrationWithTenant();
        UserRegistration ur4 = new UserRegistration();
        urwt4.setUserRegistration(ur4);
        ur4.setUser(user);
        
        return new Object[][] {
                { urwt1 }, //
                { urwt2 }, //
                { urwt3 }, //
                { urwt4 }
        };
    }
    
    private static User getUser() {
        User user = new User();
        user.setActive(true);
        user.setEmail("ron@lattice-engines.com");
        user.setFirstName("Ron");
        user.setLastName("Gonzalez");
        user.setUsername("ron@lattice-engines.com");
        return user;
    }
    
    private static Credentials getCreds() {
        Credentials creds = new Credentials();
        creds.setUsername("ron@lattice-engines.com");
        creds.setPassword(adminPasswordHash);
        return creds;
        
    }

}
