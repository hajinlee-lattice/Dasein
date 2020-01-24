package com.latticeengines.security.functionalframework;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.codec.digest.DigestUtils;
import org.hibernate.exception.ConstraintViolationException;

import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;
public class UserResourceTestNGBase extends SecurityFunctionalTestNGBase {

    @Inject
    private UserService userService;

    @Inject
    private TenantService tenantService;

    @Inject
    private GlobalAuthenticationService globalAuthenticationService;

    protected Tenant testTenant;
    protected Map<AccessLevel, User> testUsers = new HashMap<>();
    protected Map<AccessLevel, UserDocument> testUserDocs = new HashMap<>();

    protected void createTestTenant() {
        testTenant = new Tenant();
        testTenant.setName(this.getClass().getSimpleName());
        testTenant.setId(this.getClass().getSimpleName());
        destroyTestTenant();
        try {
            tenantService.registerTenant(testTenant);
        } catch (ConstraintViolationException ignore) {
            // tenant already exist
        }
    }

    protected void destroyTestTenant() {
        try {
            for (User user: userService.getUsers(testTenant.getId())) {
                makeSureUserDoesNotExist(user.getUsername());
            }
        } catch (LedpException ignore) {
            // user is already deleted
        }
        try {
            tenantService.discardTenant(testTenant);
        } catch (Exception ignore) {
            // tenant is already deleted
        }
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

        makeSureUserDoesNotExist(credentials.getUsername());

        createUser(credentials.getUsername(), user.getEmail(), user.getFirstName(), user.getLastName());

        userService.assignAccessLevel(accessLevel, testTenant.getId(), credentials.getUsername());

        return user;
    }

    protected void createTestUsers() {
        for (AccessLevel accessLevel : AccessLevel.values()) {
            User user = createTestUser(accessLevel);
            UserDocument doc = loginAndAttach(user.getUsername(), testTenant);
            testUserDocs.put(accessLevel, doc);
            testUsers.put(accessLevel, user);
        }
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
            makeSureUserDoesNotExist(user.getUsername());
            testUsers.remove(accessLevel);
        }
    }

    protected Ticket loginCreds(String username, String password){
        return globalAuthenticationService.authenticateUser(username, DigestUtils.sha256Hex(password));
    }
}
