package com.latticeengines.pls.functionalframework;

import java.util.HashMap;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.pls.globalauth.authentication.GlobalUserManagementService;
import com.latticeengines.pls.security.AccessLevel;
import com.latticeengines.pls.security.GrantedRight;
import com.latticeengines.pls.service.TenantService;
import com.latticeengines.pls.service.UserService;


public class UserResourceTestNGBase extends PlsFunctionalTestNGBase {

    @Autowired
    private TenantService tenantService;

    @Autowired
    private UserService userService;

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    protected Tenant testTenant;
    protected HashMap<AccessLevel, User> testUsers = new HashMap<>();
    protected HashMap<AccessLevel, UserDocument> testUserDocs = new HashMap<>();

    protected void createTestTenant() {
        testTenant = new Tenant();
        testTenant.setName("User Resource Test Tenant");
        testTenant.setId("USER_RESOURCE_TEST_TENANT");
        destroyTestTenant();
        tenantService.registerTenant(testTenant);
    }

    protected void destroyTestTenant() {
        try {
            for (User user: userService.getUsers(testTenant.getId())) {
                makeSureUserNoExists(user.getUsername());
            }
        } catch (LedpException e ) {
            //ignore
        }
        tenantService.discardTenant(testTenant);
    }

    protected User createTestUser(AccessLevel accessLevel) {
        User user = new User();
        user.setEmail("tester-" + UUID.randomUUID() + "@test.lattice.com");
        user.setFirstName("Test");
        user.setLastName("Tester");

        Credentials credentials = new Credentials();
        credentials.setUsername(user.getEmail());
        credentials.setPassword("WillBeModifiedImmediately");

        user.setUsername(credentials.getUsername());

        makeSureUserNoExists(credentials.getUsername());

        createUser(credentials.getUsername(), user.getEmail(), user.getFirstName(), user.getLastName());

        userService.assignAccessLevel(accessLevel, testTenant.getId(), credentials.getUsername());

        return user;
    }

    private void addOldFashionAdminUser() {
        for (GrantedRight grantedRight : AccessLevel.SUPER_ADMIN.getGrantedRights()) {
            globalUserManagementService.grantRight(grantedRight.getAuthority(), testTenant.getId(), "admin");
        }
    }

    protected void createTestUsers() {
        for (AccessLevel accessLevel : AccessLevel.values()) {
            User user = createTestUser(accessLevel);
            UserDocument doc = loginAndAttach(user.getUsername(), testTenant);
            testUserDocs.put(accessLevel, doc);
            testUsers.put(accessLevel, user);
        }

        addOldFashionAdminUser();
    }

    protected void switchToAccessLevel(AccessLevel accessLevel) {
        useSessionDoc(testUserDocs.get(accessLevel));
    }

    protected void destroyTestUsers() {
        for (AccessLevel accessLevel : AccessLevel.values()) {
            destroyTestUser(accessLevel);
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
