package com.latticeengines.pls.functionalframework;

import java.util.HashMap;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.pls.security.AccessLevel;
import com.latticeengines.pls.service.TenantService;
import com.latticeengines.pls.service.UserService;


public class UserResourceTestNGBase extends PlsFunctionalTestNGBase {

    @Autowired
    private TenantService tenantService;

    @Autowired
    private UserService userService;

    protected Tenant testTenant;
    protected HashMap<AccessLevel, User> testUsers;
    protected HashMap<AccessLevel, UserDocument> testUserDocs;

    protected void createTestTenant() {
        testTenant = new Tenant();
        testTenant.setName("Test Tenant");
        testTenant.setId("Test_" + UUID.randomUUID().toString());
        tenantService.registerTenant(testTenant);
    }

    protected void destroyTestTenant() {
        tenantService.discardTenant(testTenant);
    }

    protected void createTestUser(AccessLevel accessLevel) {
        if (testTenant == null) { createTestTenant(); }

        User user = new User();
        user.setEmail("test" + UUID.randomUUID().toString() + "@test.lattice.local");
        user.setFirstName("Test");
        user.setLastName("Tester");
        user.setPhoneNumber("650-555-5555");
        user.setTitle("Silly Tester");

        Credentials credentials = new Credentials();
        credentials.setUsername(user.getEmail());
        credentials.setPassword("WillBeModifiedImmediately");

        user.setUsername(credentials.getUsername());

        createUser(credentials.getUsername(), user.getEmail(), user.getFirstName(), user.getLastName());

        userService.assignAccessLevel(accessLevel, testTenant.getId(), credentials.getUsername());


        UserDocument doc = loginAndAttach(credentials.getUsername(), testTenant);
        testUserDocs.put(accessLevel, doc);
        testUsers.put(accessLevel, user);
    }

    protected void switchToAccessLevel(AccessLevel accessLevel) {
        useSessionDoc(testUserDocs.get(accessLevel));
    }

    protected void destroyTestUsers() {
        for (AccessLevel accessLevel : AccessLevel.values()) {
            destroyTestUser(accessLevel);
        }
        if (testTenant != null) {
            tenantService.discardTenant(testTenant);
        }
    }

    protected void destroyTestUser(AccessLevel accessLevel) {
        if (testUserDocs.containsKey(accessLevel)) {
            logoutUserDoc(testUserDocs.get(accessLevel));
            testUserDocs.remove(accessLevel);
        }
        if (testUsers.containsKey(accessLevel)) {
            User user = testUsers.get(accessLevel);
            makeSureUserNoExists(user.getUsername());
            testUsers.remove(accessLevel);
        }
    }
}
