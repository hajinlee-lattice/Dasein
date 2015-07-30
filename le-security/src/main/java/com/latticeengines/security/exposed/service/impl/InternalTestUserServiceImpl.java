package com.latticeengines.security.exposed.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.InternalTestUserService;
import com.latticeengines.security.exposed.service.UserService;

@Component("internalTestUserService")
public class InternalTestUserServiceImpl implements InternalTestUserService {
    protected static final String ADMIN_USERNAME = "bnguyen@lattice-engines.com";
    protected static final String ADMIN_PASSWORD = "tahoe";
    protected static final String ADMIN_PASSWORD_HASH = "mE2oR2b7hmeO1DpsoKuxhzx/7ODE9at6um7wFqa7udg=";
    protected static final String GENERAL_USERNAME = "lming@lattice-engines.com";
    protected static final String GENERAL_PASSWORD = "admin";
    protected static final String GENERAL_PASSWORD_HASH = "EETAlfvFzCdm6/t3Ro8g89vzZo6EDCbucJMTPhYgWiE=";
    protected static final String PASSWORD_TESTER = "pls-password-tester@test.lattice-engines.ext";
    protected static final String PASSWORD_TESTER_PASSWORD = "Lattice123";
    protected static final String PASSWORD_TESTER_PASSWORD_HASH = "3OCRIbECCiTtJ8FyaNgvTjNES/eyjQUK59Z5rMCnrAk=";

    protected static final String TESTING_USER_FIRST_NAME = "Lattice";
    protected static final String TESTING_USER_LAST_NAME = "Tester";
    protected static final String SUPER_ADMIN_USERNAME = "pls-super-admin-tester@test.lattice-engines.com";
    protected static final String INTERNAL_ADMIN_USERNAME = "pls-internal-admin-tester@test.lattice-engines.com";
    protected static final String INTERNAL_USER_USERNAME = "pls-internal-user-tester@test.lattice-engines.com";
    protected static final String EXTERNAL_ADMIN_USERNAME = "pls-external-admin-tester@test.lattice-engines.ext";
    protected static final String EXTERNAL_USER_USERNAME = "pls-external-user-tester@test.lattice-engines.ext";
    protected static final String EXTERNAL_USER_USERNAME_1 = "pls-external-user-tester-1@test.lattice-engines.ext";

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

    private void createUser(String username, String email, String firstName, String lastName, String password) {
        try {
            User user1 = new User();
            user1.setFirstName(firstName);
            user1.setLastName(lastName);
            user1.setEmail(email);

            Credentials user1Creds = new Credentials();
            user1Creds.setUsername(username);
            user1Creds.setPassword(password);
            globalUserManagementService.registerUser(user1, user1Creds);
        } catch (Exception e) {
            // user already created
        }
    }

    @Override
    public void deleteUserWithUsername(String username) {
        globalUserManagementService.deleteUser(username);
    }

    @Override
    public Map<AccessLevel, User> createAllTestUsersIfNecessaryAndReturnStandardTestersAtEachAccessLevel() {
        if (shouldRecreateUserWithUsernameAndPassword(ADMIN_USERNAME, ADMIN_PASSWORD)) {
            globalUserManagementService.deleteUser("bnguyen");
            globalUserManagementService.deleteUser(ADMIN_USERNAME);
            createUser(ADMIN_USERNAME, ADMIN_USERNAME, "Super", "User", ADMIN_PASSWORD_HASH);
        }

        if (shouldRecreateUserWithUsernameAndPassword(GENERAL_USERNAME, GENERAL_PASSWORD)) {
            globalUserManagementService.deleteUser("lming");
            globalUserManagementService.deleteUser(GENERAL_USERNAME);
            createUser(GENERAL_USERNAME, GENERAL_USERNAME, "General", "User", GENERAL_PASSWORD_HASH);
        }

        if (shouldRecreateUserWithUsernameAndPassword(PASSWORD_TESTER, PASSWORD_TESTER_PASSWORD)) {
            globalUserManagementService.deleteUser(PASSWORD_TESTER);
            createUser(PASSWORD_TESTER, PASSWORD_TESTER, "Lattice", "Tester", PASSWORD_TESTER_PASSWORD_HASH);
        }

        Map<AccessLevel, User> testingUsers = new HashMap<>();

        for (AccessLevel level : AccessLevel.values()) {
            User user = createATestUserIfNecessary(level);
            testingUsers.put(level, user);
        }

        if (shouldRecreateUserWithUsernameAndPassword(EXTERNAL_USER_USERNAME_1, GENERAL_PASSWORD)) {
            globalUserManagementService.deleteUser(EXTERNAL_USER_USERNAME_1);
            createUser(EXTERNAL_USER_USERNAME_1, EXTERNAL_USER_USERNAME_1, "Lattice", "Tester", GENERAL_PASSWORD_HASH);
        }

        return testingUsers;
    }

    private User createATestUserIfNecessary(AccessLevel accessLevel) {
        String username;
        switch (accessLevel) {
        case SUPER_ADMIN:
            username = SUPER_ADMIN_USERNAME;
            break;
        case INTERNAL_ADMIN:
            username = INTERNAL_ADMIN_USERNAME;
            break;
        case INTERNAL_USER:
            username = INTERNAL_USER_USERNAME;
            break;
        case EXTERNAL_ADMIN:
            username = EXTERNAL_ADMIN_USERNAME;
            break;
        case EXTERNAL_USER:
            username = EXTERNAL_USER_USERNAME;
            break;
        default:
            return null;
        }

        if (shouldRecreateUserWithUsernameAndPassword(username, GENERAL_PASSWORD)) {
            User user = new User();
            user.setEmail(username);
            user.setFirstName("Lattice");
            user.setLastName("Tester");

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

}
