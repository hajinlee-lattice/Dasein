package com.latticeengines.security.exposed.globalauth.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.auth.GlobalTeam;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.TeamService;
import com.latticeengines.security.exposed.service.UserFilter;
import com.latticeengines.security.exposed.service.UserService;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;

public class GlobalUserManagementServiceImplTestNG extends SecurityFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(GlobalUserManagementServiceImplTestNG.class);

    @Inject
    private GlobalUserManagementService globalUserManagementService;

    @Inject
    private GlobalAuthenticationService globalAuthenticationService;

    @Inject
    private UserService userService;

    @Inject
    private TeamService teamService;

    private String testTenantId;
    private Ticket ticket;
    private final String testUsername = "ga_usermanagementservice_tester@test.lattice-engines.com";
    private String firstName = "First";
    private String lastName = "Last";

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        try {
            Session session = login(adminUsername, adminPassword);
            ticket = session.getTicket();
            testTenantId = session.getTenant().getId();
        } catch (Exception e) {
            createAdminUser();
            Session session = login(adminUsername, adminPassword);
            ticket = session.getTicket();
            testTenantId = session.getTenant().getId();
        }
    }

    @AfterClass(groups = {"functional", "deployment"})
    public void tearDown() throws Exception {
        globalUserManagementService.deleteUser(testUsername);
        globalAuthenticationService.discard(ticket);
        super.tearDown();
    }

    @BeforeMethod(groups = {"functional", "deployment"})
    public void beforeMethod() {
        createUser(testUsername, testUsername, "Abc", "Def");
        userService.assignAccessLevel(AccessLevel.EXTERNAL_USER, testTenantId, testUsername);
    }

    @AfterMethod(groups = {"functional", "deployment"})
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

    private void assertUserInfo(User user) {
        assertNotNull(user);

        assertEquals(user.getEmail(), testUsername);
        assertEquals(user.getUsername(), testUsername);
        assertEquals(user.getFirstName(), "Abc");
        assertEquals(user.getLastName(), "Def");
    }

    @Test(groups = "functional")
    public void getUserByEmail() {
        User user = globalUserManagementService.getUserByEmail(testUsername);
        assertUserInfo(user);
        user = globalUserManagementService.getUserByUsername(testUsername);
        assertUserInfo(user);
        Long userId = globalUserManagementService.getIdByUsername(testUsername);
        Assert.assertNotNull(userId);
    }

    @Test(groups = "functional")
    public void resetAndModifyLatticeCredentials() throws InterruptedException {
        String newPassword = globalUserManagementService.resetLatticeCredentials(testUsername);
        assertNotNull(newPassword);

        Thread.sleep(500);
        Ticket ticket = globalAuthenticationService.authenticateUser(testUsername, DigestUtils.sha256Hex(newPassword));
        assertNotNull(ticket);
        assertEquals(ticket.getTenants().size(), 1);

        assertTrue(changePassword(ticket, testUsername, DigestUtils.sha256Hex(newPassword), DigestUtils.sha256Hex
                ("admin")));

        Thread.sleep(500);
        boolean result = globalAuthenticationService.discard(ticket);
        assertTrue(result);
    }

    @Test(groups = "functional")
    public void getAllUsersForTenant() {
        int originalNumber = globalUserManagementService.getAllUsersOfTenant(testTenantId).size();
        String prefix = "Tester" + UUID.randomUUID().toString();
        AccessLevel randomAccessLevel = AccessLevel.values()[new Random().nextInt(AccessLevel.values().length)];
        for (int i = 0; i < 10; i++) {
            String username = prefix + String.valueOf(i + 1);
            createUser(username, username + "@xyz.com", firstName, lastName);
            userService.assignAccessLevel(randomAccessLevel, testTenantId, username);
        }
        try {
            List<AbstractMap.SimpleEntry<User, List<String>>> userRightsList = globalUserManagementService
                    .getAllUsersOfTenant(testTenantId);
            // this assertion may fail if multiple developers are testing
            // against the same database simultaneously.
            assertEquals(userRightsList.size() - originalNumber, 10);
            for (AbstractMap.SimpleEntry<User, List<String>> userRight : userRightsList) {
                User user = userRight.getKey();
                if (user.getUsername().contains(prefix)) {
                    assertTrue(user.getEmail().endsWith("@xyz.com"));
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

    @Test(groups = "functional")
    public void testUserRightWithTeam() {
        String username = "Tester" + UUID.randomUUID().toString() + "@lattice-engines.com";
        try {
            AccessLevel accessLevel = AccessLevel.INTERNAL_ADMIN;
            Tenant preTenant = MultiTenantContext.getTenant();
            Tenant tenant = tenantEntityMgr.findByTenantId(testTenantId);
            MultiTenantContext.setTenant(tenant);
            String teamName = "TestName_" + UUID.randomUUID().toString();
            createUser(username, username, firstName, lastName);
            userService.assignAccessLevel(accessLevel, testTenantId, username);
            teamService.createTeam(username, getGlobalTeamData(teamName, Sets.newHashSet(username)));
            userService.assignAccessLevel(accessLevel, testTenantId, username, null, null, false);
            List<GlobalTeam> globalTeams = teamService.getTeamsByUserName(username, getUser(username, accessLevel.name()));
            assertEquals(globalTeams.size(), 1);
            assertNotNull(globalTeams.get(0).getTeamMembers());
            assertEquals(globalTeams.get(0).getTeamMembers().get(0).getEmail(), username);
            //get users with global Teams
            List<User> users = userService.getUsers(testTenantId, UserFilter.TRIVIAL_FILTER, true);
            Map<String, User> userMap = users.stream().collect(Collectors.toMap(User::getUsername, User->User));
            User targetUser = userMap.get(username);
            assertNotNull(targetUser);
            assertNotNull(targetUser.getUserTeams());
            assertEquals(targetUser.getUserTeams().get(0).getTeamName(), teamName);
            User testUser = users.get(0);
            //update user with global teams
            userService.assignAccessLevel(AccessLevel.EXTERNAL_ADMIN, testTenantId, testUser.getUsername(), null,
                    null,false, true, globalTeams);
            users = userService.getUsers(testTenantId, UserFilter.TRIVIAL_FILTER, true);
            userMap = users.stream().collect(Collectors.toMap(User::getUsername, User->User));
            targetUser = userMap.get(testUser.getUsername());
            assertEquals(users.size(), 3);
            assertNotNull(targetUser.getUserTeams());
            assertEquals(targetUser.getUsername(), testUser.getUsername());
            assertEquals(targetUser.getUserTeams().get(0).getTeamName(), teamName);
            teamService.deleteTeamByTenantId();
            MultiTenantContext.setTenant(preTenant);
        } finally {
            makeSureUserDoesNotExist(username);
        }
    }
}
