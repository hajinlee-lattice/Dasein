package com.latticeengines.security.exposed.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.InternalTestUserService;
import com.latticeengines.security.exposed.service.UserService;

@Component("internalTestUserService")
public class InternalTestUserServiceImpl implements InternalTestUserService {
    private final String ADMIN_USERNAME = NamingUtils.timestamp(this.getClass().getSimpleName())
            + "@lattice-engines.com";
    private static final String ADMIN_PASSWORD = "tahoe";
    private static final String ADMIN_PASSWORD_HASH = "mE2oR2b7hmeO1DpsoKuxhzx/7ODE9at6um7wFqa7udg=";
    private static final String GENERAL_PASSWORD = "admin";
    private static final String GENERAL_PASSWORD_HASH = "EETAlfvFzCdm6/t3Ro8g89vzZo6EDCbucJMTPhYgWiE=";
    private static final String PASSWORD_TESTER = "pls-password-tester@test.lattice-engines.ext";
    private static final String PASSWORD_TESTER_PASSWORD = "Lattice123";
    private static final String PASSWORD_TESTER_PASSWORD_HASH = "3OCRIbECCiTtJ8FyaNgvTjNES/eyjQUK59Z5rMCnrAk=";

    private static final String TESTING_USER_FIRST_NAME = "Lattice";
    private static final String TESTING_USER_LAST_NAME = "Tester";
    private static final String SUPER_ADMIN_USERNAME = "pls-super-admin-tester@lattice-engines.com";
    private static final String INTERNAL_ADMIN_USERNAME = "pls-internal-admin-tester@lattice-engines.com";
    private static final String INTERNAL_USER_USERNAME = "pls-internal-user-tester@lattice-engines.com";
    private static final String EXTERNAL_ADMIN_USERNAME = "pls-external-admin-tester@lattice-engines.com";
    private static final String EXTERNAL_USER_USERNAME = "pls-external-user-tester@lattice-engines.com";
    private static final String EXTERNAL_USER_USERNAME_1 = "pls-external-user-tester-1@lattice-engines.com";
    private static final String THIRD_PARTY_USER_USERNAME = "pls-third-party-user-tester@lattice-engines.com";

    private static final Long NINETY_DAYS_IN_MILLISECONDS = 90 * 24 * 60 * 60 * 1000L;

    @Autowired
    protected GlobalAuthenticationService globalAuthenticationService;

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @Autowired
    private UserService userService;

    @Override
    public void createUser(String username, String email, String firstName, String lastName) {
        createUser(username, email, firstName, lastName, GENERAL_PASSWORD_HASH);
    }

    @Override
    public void createUser(String username, String email, String firstName, String lastName, String password) {
        try {
            User user1 = new User();
            user1.setFirstName(firstName);
            user1.setLastName(lastName);
            user1.setEmail(email);

            Credentials user1Creds = new Credentials();
            user1Creds.setUsername(username);
            user1Creds.setPassword(password);
            globalUserManagementService.registerUser(INTERNAL_ADMIN_USERNAME, user1, user1Creds);
        } catch (Exception e) {
            // user already created
        }
    }

    @Override
    public void deleteUserWithUsername(String username) {
        globalUserManagementService.deleteUser(username);
    }

    @Override
    public Map<AccessLevel, User> createAllTestUsersIfNecessaryAndReturnStandardTestersAtEachAccessLevel(
            List<Tenant> testingTenants) {
        if (shouldRecreateUserWithUsernameAndPassword(ADMIN_USERNAME, ADMIN_PASSWORD)) {
            globalUserManagementService.deleteUser(ADMIN_USERNAME);
            createUser(ADMIN_USERNAME, ADMIN_USERNAME, "Super", "User", ADMIN_PASSWORD_HASH);
            for (Tenant testingTenant : testingTenants) {
                userService.assignAccessLevel(AccessLevel.SUPER_ADMIN, testingTenant.getId(), ADMIN_USERNAME);
            }
        }

        if (shouldRecreateUserWithUsernameAndPassword(PASSWORD_TESTER, PASSWORD_TESTER_PASSWORD)) {
            globalUserManagementService.deleteUser(PASSWORD_TESTER);
            createUser(PASSWORD_TESTER, PASSWORD_TESTER, "Lattice", "Tester", PASSWORD_TESTER_PASSWORD_HASH);
            for (Tenant testingTenant : testingTenants) {
                userService.assignAccessLevel(AccessLevel.EXTERNAL_USER, testingTenant.getId(), ADMIN_USERNAME);
            }
        }

        Map<AccessLevel, User> testingUsers = new HashMap<>();

        for (AccessLevel level : AccessLevel.values()) {
            User user = createATestUserIfNecessary(level);
            for (Tenant testingTenant : testingTenants) {
                userService.assignAccessLevel(level, testingTenant.getId(), user.getUsername());
            }
            testingUsers.put(level, user);
        }

        if (shouldRecreateUserWithUsernameAndPassword(EXTERNAL_USER_USERNAME_1, GENERAL_PASSWORD)) {
            globalUserManagementService.deleteUser(EXTERNAL_USER_USERNAME_1);
            createUser(EXTERNAL_USER_USERNAME_1, EXTERNAL_USER_USERNAME_1, "Lattice", "Tester", GENERAL_PASSWORD_HASH);
            for (Tenant testingTenant : testingTenants) {
                userService.assignAccessLevel(AccessLevel.EXTERNAL_USER, testingTenant.getId(),
                        EXTERNAL_USER_USERNAME_1);
            }
        }

        return testingUsers;
    }

    private User createATestUserIfNecessary(AccessLevel accessLevel) {
        String username = getUsernameForAccessLevel(accessLevel);

        if (shouldRecreateUserWithUsernameAndPassword(username, GENERAL_PASSWORD)) {
            User user = new User();
            user.setEmail(username);
            user.setFirstName(TESTING_USER_FIRST_NAME);
            user.setLastName(TESTING_USER_LAST_NAME);

            Credentials credentials = new Credentials();
            credentials.setUsername(user.getEmail());
            credentials.setPassword("WillBeModifiedImmediately");

            user.setUsername(credentials.getUsername());

            deleteUserWithUsername(credentials.getUsername());

            createUser(credentials.getUsername(), user.getEmail(), user.getFirstName(), user.getLastName());
        }

        return globalUserManagementService.getUserByEmail(username);
    }

    private boolean shouldRecreateUserWithUsernameAndPassword(String username, String password) {
        User user = globalUserManagementService.getUserByEmail(username);
        if (user == null || !user.getUsername().equals(username)) {
            return true;
        }

        try {
            Ticket ticket = loginCreds(username, password);
            if (ticket.isMustChangePassword()
                    || System.currentTimeMillis() - ticket.getPasswordLastModified() >= NINETY_DAYS_IN_MILLISECONDS) {
                return true;
            }
        } catch (LedpException e) {
            resetUserToPassword(username, password);
        }
        return false;
    }

    private void resetUserToPassword(String username, String password) {
        String tempPassword = globalUserManagementService.resetLatticeCredentials(username);
        Ticket ticket = globalAuthenticationService.authenticateUser(username, DigestUtils.sha256Hex(tempPassword));

        Credentials oldCredentials = new Credentials();
        oldCredentials.setUsername(username);
        oldCredentials.setPassword(DigestUtils.sha256Hex(tempPassword));
        Credentials newCredentials = new Credentials();
        newCredentials.setUsername(username);
        newCredentials.setPassword(DigestUtils.sha256Hex(password));

        globalUserManagementService.modifyLatticeCredentials(ticket, oldCredentials, newCredentials);
    }

    @Override
    public Ticket loginCreds(String username, String password) {
        return globalAuthenticationService.authenticateUser(username, DigestUtils.sha256Hex(password));
    }

    @Override
    public void logoutTicket(Ticket ticket) {
        globalAuthenticationService.discard(ticket);
    }

    @Override
    public String getUsernameForAccessLevel(AccessLevel accessLevel) {
        switch (accessLevel) {
        case SUPER_ADMIN:
            return SUPER_ADMIN_USERNAME;
        case INTERNAL_ADMIN:
            return INTERNAL_ADMIN_USERNAME;
        case INTERNAL_USER:
            return INTERNAL_USER_USERNAME;
        case EXTERNAL_ADMIN:
            return EXTERNAL_ADMIN_USERNAME;
        case EXTERNAL_USER:
            return EXTERNAL_USER_USERNAME;
        case THIRD_PARTY_USER:
            return THIRD_PARTY_USER_USERNAME;
        default:
            throw new IllegalArgumentException("Unknown access level!");
        }
    }

    @Override
    public String getGeneralPassword() {
        return GENERAL_PASSWORD;
    }

}
