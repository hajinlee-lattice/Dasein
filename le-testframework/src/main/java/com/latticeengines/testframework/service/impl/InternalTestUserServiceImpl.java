package com.latticeengines.testframework.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.UserService;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;
import com.latticeengines.testframework.exposed.service.InternalTestUserService;

@Component("internalTestUserServiceNew")
public class InternalTestUserServiceImpl implements InternalTestUserService {
    private static final Long NINETY_DAYS_IN_MILLISECONDS = 90 * 24 * 60 * 60 * 1000L;

    @Autowired
    protected GlobalAuthenticationService globalAuthenticationService;

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @Autowired
    private UserService userService;

    @Override
    public void createUser(String username, String email, String firstName, String lastName) {
        createUser(username, email, firstName, lastName, TestFrameworkUtils.GENERAL_PASSWORD_HASH);
    }

    @Override
    public void createUser(String username, String email, String firstName, String lastName, String password) {
        try {
            if (userService.findByUsername(username) == null) {
                User user1 = new User();
                user1.setFirstName(firstName);
                user1.setLastName(lastName);
                user1.setEmail(email);

                Credentials user1Creds = new Credentials();
                user1Creds.setUsername(username);
                user1Creds.setPassword(password);
                globalUserManagementService.registerUser(user1, user1Creds);
            }
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
        if (shouldRecreateUserWithUsernameAndPassword(TestFrameworkUtils.PASSWORD_TESTER, TestFrameworkUtils.PASSWORD_TESTER_PASSWORD)) {
            globalUserManagementService.deleteUser(TestFrameworkUtils.PASSWORD_TESTER);
            createUser(TestFrameworkUtils.PASSWORD_TESTER, TestFrameworkUtils.PASSWORD_TESTER, "Lattice", "Tester", TestFrameworkUtils.PASSWORD_TESTER_PASSWORD_HASH);
            for (Tenant testingTenant : testingTenants) {
                userService.assignAccessLevel(AccessLevel.EXTERNAL_USER, testingTenant.getId(), TestFrameworkUtils.PASSWORD_TESTER);
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

        if (shouldRecreateUserWithUsernameAndPassword(TestFrameworkUtils.EXTERNAL_USER_USERNAME_1, TestFrameworkUtils.GENERAL_PASSWORD)) {
            globalUserManagementService.deleteUser(TestFrameworkUtils.EXTERNAL_USER_USERNAME_1);
            createUser(TestFrameworkUtils.EXTERNAL_USER_USERNAME_1, TestFrameworkUtils.EXTERNAL_USER_USERNAME_1, "Lattice", "Tester", TestFrameworkUtils.GENERAL_PASSWORD_HASH);
            for (Tenant testingTenant : testingTenants) {
                userService.assignAccessLevel(AccessLevel.EXTERNAL_USER, testingTenant.getId(),
                        TestFrameworkUtils.EXTERNAL_USER_USERNAME_1);
            }
        }

        return testingUsers;
    }

    private User createATestUserIfNecessary(AccessLevel accessLevel) {
        String username = getUsernameForAccessLevel(accessLevel);

        if (shouldRecreateUserWithUsernameAndPassword(username, TestFrameworkUtils.GENERAL_PASSWORD)) {
            User user = new User();
            user.setEmail(username);
            user.setFirstName(TestFrameworkUtils.TESTING_USER_FIRST_NAME);
            user.setLastName(TestFrameworkUtils.TESTING_USER_LAST_NAME);

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
        return TestFrameworkUtils.usernameForAccessLevel(accessLevel);
    }

    @Override
    public String getGeneralPassword() {
        return TestFrameworkUtils.GENERAL_PASSWORD;
    }
}
