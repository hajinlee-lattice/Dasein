package com.latticeengines.security.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.pls.RegistrationResult;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.pls.UserUpdateData;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.service.UserService;
import com.latticeengines.security.functionalframework.UserResourceTestNGBase;

public class UserResourceTestNG extends UserResourceTestNGBase {

    private static final String FORBIDDEN_MSG = "403 FORBIDDEN";

    @Inject
    private GlobalAuthenticationService globalAuthenticationService;

    @Inject
    private UserService userService;

    private static final AccessLevel[] LEVELS = AccessLevel.values();
    private static String usersApi;

    @BeforeClass(groups = { "functional", "deployment" })
    public void setup() throws Exception {
        createTestTenant();
        createTestUsers();
        usersApi = getRestAPIHostPort() + "/users/";
    }

    @AfterClass(groups = { "functional", "deployment" })
    public void tearDown() {
        destroyTestUsers();
        destroyTestTenant();
    }

    @Test(groups = { "functional", "deployment" }, dataProvider = "authTableProvider")
    public void registerUser(AccessLevel level, Boolean[] expectForEachTargetLevel) {//
        switchToAccessLevel(level);
        for (int i = 0; i < LEVELS.length; i++) {
            AccessLevel targetLevel = LEVELS[i];
            Boolean expectSucceed = expectForEachTargetLevel[i];
            if (expectSucceed) {
                testRegisterUserSuccess(targetLevel);
            } else {
                testRegisterUserFail(targetLevel);
            }
        }
    }

    @Test(groups = { "functional", "deployment" }, dataProvider = "authTableProvider")
    public void updateAccessLevel(AccessLevel level, Boolean[] expectForEachTargetLevel) {
        switchToAccessLevel(level);
        User user = createTestUser(AccessLevel.EXTERNAL_USER);
        for (int i = 0; i < LEVELS.length; i++) {
            AccessLevel targetLevel = LEVELS[i];
            testUpdateAccessLevel(user, targetLevel, expectForEachTargetLevel[i]);
        }
    }

    @Test(groups = { "functional", "deployment" }, dataProvider = "authTableProvider")
    public void deleteUser(AccessLevel level, Boolean[] expectForEachTargetLevel) {
        switchToAccessLevel(level);
        for (int i = 0; i < LEVELS.length; i++) {
            AccessLevel targetLevel = LEVELS[i];
            testDeleteUser(targetLevel, expectForEachTargetLevel[i]);
        }
    }

    @DataProvider(name="authTableProvider")
    private static Object[][] authTableProvider() {
        return new Object[][] { //
            { AccessLevel.SUPER_ADMIN, new Boolean[] {true, true, true, true, true, true} },
            { AccessLevel.INTERNAL_ADMIN, new Boolean[] {true, true, true, true, true, false} },
            { AccessLevel.INTERNAL_USER, new Boolean[] {false, false, false, false, false, false} },
            { AccessLevel.EXTERNAL_ADMIN, new Boolean[] {true, true, true, false, false, false} },
            { AccessLevel.EXTERNAL_USER, new Boolean[] {false, false, false, false, false, false} },
            { AccessLevel.THIRD_PARTY_USER, new Boolean[] {false, false, false, false, false, false} },
        };
    }

    @Test(groups = { "functional", "deployment" })
    public void validateNewUser() {
        switchToAccessLevel(AccessLevel.SUPER_ADMIN);
        testConflictingUserInTenant();
        testConflictingUserOutsideTenant();
    }

    @Test(groups = { "functional", "deployment" }, dataProvider = "getAllUsersProvider")
    public void getAllUsers(AccessLevel level, Boolean expectSucceed, int visibleUsers) throws Exception {
        tearDown();
        setup();

        switchToAccessLevel(level);
        if (expectSucceed) {
            String json = restTemplate.getForObject(usersApi, String.class);
            ResponseDocument<ArrayNode> response = ResponseDocument.generateFromJSON(json, ArrayNode.class);
            assertNotNull(response);
            ArrayNode users = response.getResult();
            assertEquals(users.size(), visibleUsers);
        } else {
            boolean exception = false;
            try {
                restTemplate.getForObject(usersApi, String.class);
            } catch (RuntimeException e) {
                exception = true;
                assertEquals(e.getMessage(), FORBIDDEN_MSG);
            }
            assertTrue(exception);
        }
    }

    @DataProvider(name="getAllUsersProvider")
    public static Object[][] getAllUsersProvider() {
        return new Object[][] {
                { AccessLevel.SUPER_ADMIN, true, 6 },
                { AccessLevel.INTERNAL_ADMIN, true, 6 },
                { AccessLevel.INTERNAL_USER, false, 0 },
                { AccessLevel.EXTERNAL_ADMIN, true, 2 },
                { AccessLevel.EXTERNAL_USER, false, 0 },
                { AccessLevel.THIRD_PARTY_USER, false, 0 },
        };
    }

    @Test(groups = { "functional", "deployment" })
    public void changePassword() {
        switchToAccessLevel(AccessLevel.SUPER_ADMIN);
        testChangePassword(AccessLevel.THIRD_PARTY_USER);
        testChangePassword(AccessLevel.EXTERNAL_USER);
        testChangePassword(AccessLevel.EXTERNAL_ADMIN);
        testChangePassword(AccessLevel.INTERNAL_USER);
        testChangePassword(AccessLevel.INTERNAL_ADMIN);
        testChangePassword(AccessLevel.SUPER_ADMIN);
    }


    @SuppressWarnings("rawtypes")
    @Test(groups = { "functional", "deployment" })
    public void deleteUserWithShortEmail() {
        switchToAccessLevel(AccessLevel.SUPER_ADMIN);
        String shortEmail = "a@b.c";
        makeSureUserDoesNotExist(shortEmail);
        createUser(shortEmail, shortEmail, "Short", "Email");
        userService.assignAccessLevel(AccessLevel.INTERNAL_USER, testTenant.getId(), shortEmail);

        String url = usersApi + "\"" + shortEmail + "\"";
        ResponseDocument response = sendHttpDeleteForObject(restTemplate, url, ResponseDocument.class);
        assertTrue(response.isSuccess());

        makeSureUserDoesNotExist(shortEmail);
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = { "functional", "deployment" })
    public void stringifiedUserName_updateAccessLevel_acessLevelSuccessfullyUpdated() {
        switchToAccessLevel(AccessLevel.SUPER_ADMIN);
        User user = createTestUser(AccessLevel.EXTERNAL_USER);
        UserUpdateData data = new UserUpdateData();
        data.setAccessLevel(user.getAccessLevel());

        String url = usersApi + "\"" + user.getUsername() + "\"";
        ResponseDocument response = sendHttpPutForObject(restTemplate, url, data, ResponseDocument.class);
        assertTrue(response.isSuccess());
    }

    private UserRegistration createUserRegistration() {
        UserRegistration userReg = new UserRegistration();

        User user = new User();
        user.setEmail("test" + UUID.randomUUID().toString() + "@test.com");
        user.setFirstName("Test");
        user.setLastName("Tester");
        user.setPhoneNumber("650-555-5555");
        user.setTitle("Silly Tester");
        user.setAccessLevel(AccessLevel.EXTERNAL_USER.name());

        Credentials creds = new Credentials();
        creds.setUsername(user.getEmail());
        creds.setPassword("WillBeModifiedImmediately");

        user.setUsername(creds.getUsername());

        userReg.setUser(user);
        userReg.setCredentials(creds);

        return userReg;
    }

    private void testRegisterUserSuccess(AccessLevel accessLevel) {
        UserRegistration userReg = createUserRegistration();
        userReg.getUser().setAccessLevel(accessLevel.name());
        makeSureUserDoesNotExist(userReg.getCredentials().getUsername());
        MultiValueMap<String, String> headers = new LinkedMultiValueMap<>();
        headers.add(Constants.SET_TEMP_PASS, String.valueOf(Boolean.TRUE));
        HttpEntity<UserRegistration> httpEntity = new HttpEntity<>(userReg, headers);
        ResponseEntity<String> json = restTemplate.exchange(usersApi, HttpMethod.POST, httpEntity, String.class);
        ResponseDocument<RegistrationResult> response = ResponseDocument.generateFromJSON(json.getBody(),
                RegistrationResult.class);
        assertNotNull(response);
        assertTrue(response.isSuccess());
        assertNotNull(response.getResult().getPassword());

        User newUser = userService.findByEmail(userReg.getUser().getEmail());
        assertNotNull(newUser);
        assertEquals(newUser.getFirstName(), userReg.getUser().getFirstName());
        assertEquals(newUser.getLastName(), userReg.getUser().getLastName());
        assertEquals(newUser.getPhoneNumber(), userReg.getUser().getPhoneNumber());
        assertEquals(newUser.getTitle(), userReg.getUser().getTitle());

        String password = response.getResult().getPassword();
        Ticket ticket = globalAuthenticationService.authenticateUser(userReg.getUser().getUsername(),
                DigestUtils.sha256Hex(password));
        assertEquals(ticket.getTenants().size(), 1);
        globalAuthenticationService.discard(ticket);

        makeSureUserDoesNotExist(userReg.getCredentials().getUsername());
    }

    private void testRegisterUserFail(AccessLevel accessLevel) {
        UserRegistration userReg = createUserRegistration();
        userReg.getUser().setAccessLevel(accessLevel.name());
        makeSureUserDoesNotExist(userReg.getCredentials().getUsername());

        boolean exception = false;
        try {
            restTemplate.postForObject(usersApi, userReg, String.class);
        } catch (RuntimeException e) {
            exception = true;
            assertEquals(e.getMessage(), FORBIDDEN_MSG);
        }
        assertTrue(exception);
        assertNull(userService.findByEmail(userReg.getUser().getEmail()));

        makeSureUserDoesNotExist(userReg.getCredentials().getUsername());
    }

    @SuppressWarnings("rawtypes")
    private void testDeleteUser(AccessLevel accessLevel, boolean expected) {
        User user = createTestUser(accessLevel);
        String url = usersApi+ user.getUsername() + "/";

        if (expected) {
            ResponseDocument response = sendHttpDeleteForObject(restTemplate, url, ResponseDocument.class);
            assertTrue(response.isSuccess());
            assertNull(userService.findByEmail(user.getEmail()));
        } else {
            boolean exception = false;
            try {
                sendHttpDeleteForObject(restTemplate, url, ResponseDocument.class);
            } catch (RuntimeException e) {
                exception = true;
                assertEquals(e.getMessage(), FORBIDDEN_MSG);
            }
            assertTrue(exception);
            assertNotNull(userService.findByEmail(user.getEmail()));
        }

        makeSureUserDoesNotExist(user.getUsername());
    }

    private void testUpdateAccessLevel(User user, AccessLevel targetLevel, boolean expectSuccess) {
        if (expectSuccess) {
            updateAccessLevelWithSufficientPrivilege(user, targetLevel);
        } else {
            updateAccessLevelWithoutSufficientPrivilege(user, targetLevel);
        }
    }

    @SuppressWarnings("rawtypes")
    private void updateAccessLevelWithSufficientPrivilege(User user, AccessLevel accessLevel) {
        UserUpdateData data = new UserUpdateData();
        data.setAccessLevel(accessLevel.name());

        String url = usersApi + user.getUsername() + "/";
        ResponseDocument response = sendHttpPutForObject(restTemplate, url, data, ResponseDocument.class);
        assertTrue(response.isSuccess());

        AccessLevel resultLevel = userService.getAccessLevel(testTenant.getId(), user.getUsername());
        assertEquals(accessLevel, resultLevel);
    }

    private void updateAccessLevelWithoutSufficientPrivilege(User user, AccessLevel accessLevel) {
        UserUpdateData data = new UserUpdateData();
        data.setAccessLevel(accessLevel.name());

        String url = usersApi + user.getUsername() + "/";
        boolean exception = false;
        try {
            sendHttpPutForObject(restTemplate, url, data, ResponseDocument.class);
        } catch (RuntimeException e) {
            exception = true;
            assertEquals(e.getMessage(), FORBIDDEN_MSG);
        }
        assertTrue(exception);
    }

    private void testConflictingUserInTenant() {
        User existingUser = createTestUser(AccessLevel.EXTERNAL_USER);

        UserRegistration uReg = createUserRegistration();
        uReg.getUser().setEmail(existingUser.getEmail());

        try {
            restTemplate.postForObject(usersApi, uReg, String.class);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "400 BAD_REQUEST");
        }
        // ResponseDocument<RegistrationResult> response =
        // ResponseDocument.generateFromJSON(json,
        // RegistrationResult.class);
        // assertNotNull(response);
        // assertFalse(response.getResult().isValid());
        // assertNull(response.getResult().getConflictingUser(),
        // "When conflict with another use in the same tenant, should not show
        // the conflicting user");

        makeSureUserDoesNotExist(existingUser.getUsername());
    }

    private void testConflictingUserOutsideTenant() {
        User existingUser = createTestUser(AccessLevel.EXTERNAL_USER);
        userService.resignAccessLevel(testTenant.getId(), existingUser.getUsername());

        UserRegistration uReg = createUserRegistration();
        uReg.getUser().setEmail(existingUser.getEmail());

        try {
            restTemplate.postForObject(usersApi, uReg, String.class);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "400 BAD_REQUEST");
        }
        // ResponseDocument<RegistrationResult> response =
        // ResponseDocument.generateFromJSON(json,
        // RegistrationResult.class);
        // assertNotNull(response);
        // assertFalse(response.getResult().isValid());
        // // when conflict with another use outside the same tenant,
        // // show the conflicting user if its access level is lower than the
        // // current logged in user
        // User user = response.getResult().getConflictingUser();
        // assertEquals(user.getFirstName(), existingUser.getFirstName());
        // assertEquals(user.getLastName(), existingUser.getLastName());
        // assertEquals(user.getEmail(), existingUser.getEmail());
        // assertEquals(user.getUsername(), existingUser.getUsername());

        makeSureUserDoesNotExist(existingUser.getUsername());
    }

    @SuppressWarnings("rawtypes")
    private void testChangePassword(AccessLevel accessLevel) {
        User user = createTestUser(accessLevel);
        UserDocument doc = loginAndAttach(user.getUsername());
        useSessionDoc(doc);

        UserUpdateData data = new UserUpdateData();
        data.setOldPassword(DigestUtils.sha256Hex("wrong"));
        data.setNewPassword(DigestUtils.sha256Hex("newpass"));
        String url = usersApi + "creds";
        try {
            sendHttpPutForObject(restTemplate, url, data, ResponseDocument.class);
        } catch (RuntimeException e) {
            assertEquals(e.getMessage(), "401");
        }

        data.setOldPassword(DigestUtils.sha256Hex(generalPassword));
        ResponseDocument response = sendHttpPutForObject(restTemplate, url, data, ResponseDocument.class);
        assertTrue(response.isSuccess());

        logoutUserDoc(doc);

        Ticket ticket = loginCreds(user.getUsername(), "newpass");
        logoutTicket(ticket);

        makeSureUserDoesNotExist(user.getUsername());
    }

    @SuppressWarnings({ "unchecked" })
    @Test(groups = { "functional", "deployment" })
    public void testTenantRightAfterExpiration() {
        switchToAccessLevel(AccessLevel.SUPER_ADMIN);

        User existingUser = createTestUser(AccessLevel.INTERNAL_ADMIN);
        ResponseDocument<List<?>> usersResponse = restTemplate.getForObject(usersApi, ResponseDocument.class);
        assertNotNull(usersResponse);
        List<User> users = JsonUtils.convertList(usersResponse.getResult(), User.class);
        List<String> names = users.stream().map(user -> user.getUsername()).collect(Collectors.toList());
        // ensure existing exists in returned list
        Assert.assertTrue(names.contains(existingUser.getUsername()));

        updateExpirationDate(existingUser, AccessLevel.INTERNAL_ADMIN, false);
        makeSureUserDoesNotExist(existingUser.getUsername());

        // verify external user is not subject to expiration date
        existingUser = createTestUser(AccessLevel.EXTERNAL_USER);
        updateExpirationDate(existingUser, AccessLevel.EXTERNAL_ADMIN, true);
        makeSureUserDoesNotExist(existingUser.getUsername());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void updateExpirationDate(User existingUser, AccessLevel accessLevel, boolean exists) {
        UserUpdateData data = new UserUpdateData();
        data.setAccessLevel(accessLevel.name());
        // update expiration date to now
        data.setExpirationDate(System.currentTimeMillis());
        String url = usersApi + existingUser.getUsername() + "/";

        ResponseDocument response = sendHttpPutForObject(restTemplate, url, data, ResponseDocument.class);
        assertTrue(response.isSuccess());

        // verify the user whether tenant exists in get users api
        ResponseDocument<List<?>> usersResponse = restTemplate.getForObject(usersApi, ResponseDocument.class);
        assertNotNull(usersResponse);
        List<User> users = JsonUtils.convertList(usersResponse.getResult(), User.class);
        List<String> names = users.stream().map(user -> user.getUsername()).collect(Collectors.toList());
        // ensure existing user not exist in returned users
        Assert.assertEquals(names.contains(existingUser.getUsername()), exists);
    }
}
