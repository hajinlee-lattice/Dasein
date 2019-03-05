package com.latticeengines.security.exposed.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.saml.LoginValidationResponse;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.globalauth.GlobalTenantManagementService;
import com.latticeengines.security.exposed.service.UserService;
import com.latticeengines.security.exposed.util.SamlIntegrationRole;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;

public class UserServiceImplTestNG extends SecurityFunctionalTestNGBase {

    @Inject
    GlobalTenantManagementService globalTenantManagementService;

    @Inject
    UserService userService;

    private Tenant tenant;
    private Tenant anotherTenant;
    private final UserRegistration uReg = createUserRegistration();
    private final LoginValidationResponse samlUser = createExternalUserFromSamlResponse();


    @BeforeClass(groups = "functional")
    public void setup() {
        tenant = new Tenant();
        tenant.setName("UserService Test Tenant");
        tenant.setId("USERSERVICE_TEST_TENANT");

        globalTenantManagementService.discardTenant(tenant);
        globalTenantManagementService.registerTenant(tenant);

        makeSureUserDoesNotExist(uReg.getCredentials().getUsername());
        makeSureUserDoesNotExist(samlUser.getUserId());

        createUser(uReg.getCredentials().getUsername(), uReg.getUser().getEmail(), uReg.getUser().getFirstName(), uReg
                .getUser().getLastName());

        anotherTenant = new Tenant();
        anotherTenant.setName("UserService Test Another Tenant");
        anotherTenant.setId("USERSERVICE_TEST_ANOTHER_TENANT");

        globalTenantManagementService.discardTenant(anotherTenant);
        globalTenantManagementService.registerTenant(anotherTenant);
    }

    @AfterClass(groups = { "functional" })
    public void tearDown() {
        makeSureUserDoesNotExist(uReg.getCredentials().getUsername());
        makeSureUserDoesNotExist(samlUser.getUserId());
        globalTenantManagementService.discardTenant(tenant);
        globalTenantManagementService.discardTenant(anotherTenant);
    }

    @Test(groups = "functional")
    public void testGrantAndRevokeAccessLevel() {
        AccessLevel level = userService.getAccessLevel(tenant.getId(), uReg.getCredentials().getUsername());
        assertNull(level);
        for (AccessLevel accessLevel : AccessLevel.values()) {
            userService.assignAccessLevel(accessLevel, tenant.getId(), uReg.getCredentials().getUsername());
            level = userService.getAccessLevel(tenant.getId(), uReg.getCredentials().getUsername());
            assertEquals(level, accessLevel);
        }
        userService.resignAccessLevel(tenant.getId(), uReg.getCredentials().getUsername());
        level = userService.getAccessLevel(tenant.getId(), uReg.getCredentials().getUsername());
        assertNull(level);
    }

    @Test(groups = "functional", dependsOnMethods = {"testGrantAndRevokeAccessLevel"})
    public void testAddUserAccessLevel() {
        AccessLevel level = userService.getAccessLevel(tenant.getId(), uReg.getCredentials().getUsername());
        assertNull(level);
        userService.assignAccessLevel(AccessLevel.SUPER_ADMIN, tenant.getId(), uReg.getCredentials().getUsername());
        level = userService.getAccessLevel(tenant.getId(), uReg.getCredentials().getUsername());
        assertEquals(level, AccessLevel.SUPER_ADMIN);
        userService.addUserAccessLevel("default", uReg.getUser().getEmail(), AccessLevel.INTERNAL_ADMIN);
        level = userService.getAccessLevel(anotherTenant.getId(), uReg.getCredentials().getUsername());
        assertEquals(level, AccessLevel.INTERNAL_ADMIN);
    }

    @Test(groups = "functional", dependsOnMethods = {"testAddUserAccessLevel"})
    public void testFindUser() {
        User user = userService.findByUsername(uReg.getCredentials().getUsername());
        assertNotNull(user);

        user = userService.findByEmail(uReg.getUser().getEmail());
        assertNotNull(user);
    }

    @Test(groups = "functional")
    public void testCreateSamlExternalUser() {
        assertTrue(userService.upsertSamlIntegrationUser(null, samlUser, tenant.getId()));
        User user = userService.findByUsername(samlUser.getUserId());
        assertNotNull(user);
        user = userService.findByEmail(samlUser.getUserId());
        assertNotNull(user);
    }

    @Test(groups = "functional", dependsOnMethods = {"testCreateSamlExternalUser"})
    public void testSamlUserAccessLevel() {
        AccessLevel level = userService.getAccessLevel(tenant.getId(), samlUser.getUserId());
        assertEquals(level, AccessLevel.EXTERNAL_USER);

        // Simulate the SAML Login by changing User Roles on OKTA
        List<String> externalRoles = new ArrayList<>();
        externalRoles.add(SamlIntegrationRole.LATTICE_ADMIN.name());
        samlUser.setUserRoles(externalRoles);

        assertTrue(userService.upsertSamlIntegrationUser(null, samlUser, tenant.getId()));
        level = userService.getAccessLevel(tenant.getId(), samlUser.getUserId());
        assertEquals(level, AccessLevel.EXTERNAL_ADMIN);
    }

    private UserRegistration createUserRegistration() {
        UserRegistration userReg = new UserRegistration();

        User user = new User();
        user.setEmail("test" + UUID.randomUUID().toString() + "@test.com");
        user.setFirstName("Test");
        user.setLastName("Tester");
        user.setPhoneNumber("650-555-5555");
        user.setTitle("CEO");

        Credentials creds = new Credentials();
        creds.setUsername(user.getEmail());
        creds.setPassword("WillBeModifiedImmediately");

        user.setUsername(creds.getUsername());

        userReg.setUser(user);
        userReg.setCredentials(creds);

        return userReg;
    }

    private LoginValidationResponse createExternalUserFromSamlResponse() {
        LoginValidationResponse samlUser = new LoginValidationResponse();
        samlUser.setUserId("samltest" + UUID.randomUUID().toString() + "@test.com");
        samlUser.setFirstName("Test SamlUser");
        samlUser.setLastName("SamlTester");

        return samlUser;
    }

}
