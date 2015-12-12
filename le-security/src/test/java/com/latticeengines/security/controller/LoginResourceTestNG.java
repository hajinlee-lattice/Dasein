package com.latticeengines.security.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.globalauth.GlobalTenantManagementService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.InternalTestUserService;
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

    }

    @Test(groups = { "functional", "deployment" })
    public void login() {
        Credentials creds = new Credentials();
        creds.setUsername(adminUsername);
        creds.setPassword(DigestUtils.sha256Hex(adminPassword));

        LoginDocument loginDoc = restTemplate
                .postForObject(getRestAPIHostPort() + "/login", creds, LoginDocument.class);
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

}
