package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.TenantService;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.UserService;

public class UserServiceImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    TenantService tenantService;

    @Autowired
    UserService userService;

    private Tenant tenant;
    private final UserRegistration uReg = createUserRegistration();

    @BeforeClass(groups = "functional")
    public void setup() {
        tenant = new Tenant();
        tenant.setName("UserService Test Tenant");
        tenant.setId("USERSERVICE_TEST_TENANT");

        tenantService.discardTenant(tenant);
        tenantService.registerTenant(tenant);

        deleteUserWithUsername(uReg.getCredentials().getUsername());

        createUser(uReg.getCredentials().getUsername(), uReg.getUser().getEmail(), uReg.getUser().getFirstName(), uReg
                .getUser().getLastName());
    }

    @AfterClass(groups = { "functional" })
    public void tearDown() {
        deleteUserWithUsername(uReg.getCredentials().getUsername());
        tenantService.discardTenant(tenant);
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

}
