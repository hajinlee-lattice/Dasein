package com.latticeengines.security.exposed.globalauth.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.AbstractMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.UserService;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;

public class GlobalUserManagementServiceImplTestNG extends SecurityFunctionalTestNGBase {

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    @Autowired
    private UserService userService;

    private String testTenantId;
    private Ticket ticket;
    private final String testUsername = "ga_usermanagementservice_tester@test.lattice-engines.com";

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        Session session = login(adminUsername, adminPassword);
        ticket = session.getTicket();
        testTenantId = session.getTenant().getId();
    }

    @AfterClass(groups = { "functional", "deployment" })
    public void tearDown() {
        globalUserManagementService.deleteUser(testUsername);
        globalAuthenticationService.discard(ticket);
    }

    @BeforeMethod(groups = { "functional", "deployment" })
    public void beforeMethod() {
        createUser(testUsername, testUsername, "Abc", "Def");
        userService.assignAccessLevel(AccessLevel.EXTERNAL_USER, testTenantId, testUsername);
    }

    @AfterMethod(groups = { "functional", "deployment" })
    public void afterMethod() {
        globalUserManagementService.deleteUser(testUsername);
    }


    @Test(groups = "functional")
    public void deleteUser() {
        Session session = login(testUsername);
        assertNotNull(session);
        globalUserManagementService.deleteUser(testUsername);

        boolean exception = false;
        try {
            login(testUsername);
        } catch (Exception e) {
            exception = true;
        }
        assertTrue(exception);
    }

    @Test(groups = "functional")
    public void getUserByEmail() {
        User user = globalUserManagementService.getUserByEmail(testUsername);
        assertNotNull(user);

        assertEquals(user.getEmail(), testUsername);
        assertEquals(user.getUsername(), testUsername);
        assertEquals(user.getFirstName(), "Abc");
        assertEquals(user.getLastName(), "Def");
    }

    @Test(groups = "functional")
    public void resetLatticeCredentials() {
        String newPassword = globalUserManagementService.resetLatticeCredentials(testUsername);
        assertNotNull(newPassword);

        Ticket ticket = globalAuthenticationService.authenticateUser(testUsername, DigestUtils.sha256Hex(newPassword));
        assertNotNull(ticket);
        assertEquals(ticket.getTenants().size(), 1);

        boolean result = globalAuthenticationService.discard(ticket);
        assertTrue(result);
    }

    @Test(groups = "functional")
    public void getAllUsersForTenant() {
        int originalNumber = globalUserManagementService.getAllUsersOfTenant(testTenantId).size();

        String prefix = "Tester" + UUID.randomUUID().toString();
        String firstName = "First";
        String lastName = "Last";

        AccessLevel randomAccessLevel = AccessLevel.values()[new Random().nextInt(AccessLevel.values().length)];

        for (int i = 0; i < 10; i++) {
            String username = prefix + String.valueOf(i + 1);
            createUser(username, username + "@xyz.com", firstName, lastName);
            userService.assignAccessLevel(randomAccessLevel, testTenantId, username);
        }
        try {
            List<AbstractMap.SimpleEntry<User, List<String>>> userRightsList =
                    globalUserManagementService.getAllUsersOfTenant(testTenantId);

            // this assertion may fail if multiple developers are testing against the same database simultaneously.
            assertEquals(userRightsList.size() - originalNumber, 10);

            for (AbstractMap.SimpleEntry<User, List<String>> userRight : userRightsList) {
                User user = userRight.getKey();
                if (user.getUsername().contains(prefix)) {
                    assertEquals(user.getEmail(), user.getUsername() + "@xyz.com");
                    assertEquals(user.getFirstName(), firstName);
                    assertEquals(user.getLastName(), lastName);

                    List<String> rights = userRight.getValue();
                    assertEquals(rights.size(), 1);
                    assertEquals(AccessLevel.valueOf(rights.get(0)), randomAccessLevel);
                }
            }

        } finally {
            for (int i = 0; i < 10; i++) {
                makeSureUserDoesNotExist(prefix + String.valueOf(i + 1));
            }
        }
    }
}
