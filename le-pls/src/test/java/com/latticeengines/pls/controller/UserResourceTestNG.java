package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.UUID;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.latticeengines.domain.exposed.pls.RegistrationResult;
import com.latticeengines.domain.exposed.pls.ResponseDocument;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.pls.UserUpdateData;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.pls.functionalframework.UserResourceTestNGBase;
import com.latticeengines.pls.globalauth.authentication.GlobalAuthenticationService;
import com.latticeengines.pls.globalauth.authentication.GlobalUserManagementService;
import com.latticeengines.pls.security.AccessLevel;
import com.latticeengines.pls.service.UserService;


public class UserResourceTestNG extends UserResourceTestNGBase {
    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @Autowired
    private UserService userService;

    @BeforeClass(groups = { "functional", "deployment" })
    public void setup() throws Exception {
        createTestTenant();
        createTestUsers();
    }

    @AfterClass(groups = { "functional", "deployment" })
    public void tearDown() {
        destroyTestUsers();
        destroyTestTenant();
    }

    @BeforeMethod(groups = { "functional", "deployment" })
    public void beforeMethod() {
        switchToAccessLevel(AccessLevel.SUPER_ADMIN);
    }

    @Test(groups = { "functional", "deployment" })
    public void registerUser() {
        switchToAccessLevel(AccessLevel.SUPER_ADMIN);
        testRegisterUserSuccess();

        switchToAccessLevel(AccessLevel.INTERNAL_ADMIN);
        testRegisterUserSuccess();

        switchToAccessLevel(AccessLevel.EXTERNAL_ADMIN);
        testRegisterUserSuccess();

        switchToAccessLevel(AccessLevel.INTERNAL_USER);
        testRegisterUserFail();

        switchToAccessLevel(AccessLevel.EXTERNAL_USER);
        testRegisterUserFail();
    }

    @Test(groups = { "functional", "deployment" })
    public void validateNewUser() throws JsonProcessingException {
        // Conflict with a user in the same tenant
        testConflictingUserInTenant();
        testConflictingUserOutsideTenant(AccessLevel.EXTERNAL_USER, AccessLevel.SUPER_ADMIN);
    }

    @Test(groups = { "functional", "deployment" })
    public void getAllUsers() {
        switchToAccessLevel(AccessLevel.EXTERNAL_USER);
        testGetAllUsersFail();

        switchToAccessLevel(AccessLevel.INTERNAL_USER);
        testGetAllUsersFail();

//        testGetAllUsers(AccessLevel.EXTERNAL_ADMIN, true, 1);
//        testGetAllUsers(AccessLevel.INTERNAL_ADMIN, true, 4);
//        testGetAllUsers(AccessLevel.SUPER_ADMIN, true, 5);

        //TODO:song this will be a wrong assertion after Access Level feature is completed
        switchToAccessLevel(AccessLevel.SUPER_ADMIN);
        testGetAllUsersSuccess(4);
    }

    @Test(groups = { "functional", "deployment" })
    public void changePassword() {
        testChangePassword(AccessLevel.EXTERNAL_USER);
        testChangePassword(AccessLevel.EXTERNAL_ADMIN);
        testChangePassword(AccessLevel.INTERNAL_USER);
        testChangePassword(AccessLevel.INTERNAL_ADMIN);
        testChangePassword(AccessLevel.SUPER_ADMIN);
    }

    @Test(groups = { "functional", "deployment" })
    public void updateAccessLevelWithSuperAdmin() {
        switchToAccessLevel(AccessLevel.SUPER_ADMIN);
        testUpdateAccessLevel(AccessLevel.EXTERNAL_USER, true);
        testUpdateAccessLevel(AccessLevel.EXTERNAL_ADMIN, true);
        testUpdateAccessLevel(AccessLevel.INTERNAL_USER, true);
        testUpdateAccessLevel(AccessLevel.INTERNAL_ADMIN, true);
        testUpdateAccessLevel(AccessLevel.SUPER_ADMIN, true);
    }

    private UserRegistration createUserRegistration() {
        UserRegistration userReg = new UserRegistration();

        User user = new User();
        user.setEmail("test" + UUID.randomUUID().toString() + "@test.com");
        user.setFirstName("Test");
        user.setLastName("Tester");
        user.setPhoneNumber("650-555-5555");
        user.setTitle("Silly Tester");

        Credentials creds = new Credentials();
        creds.setUsername(user.getEmail());
        creds.setPassword("WillBeModifiedImmediately");

        user.setUsername(creds.getUsername());

        userReg.setUser(user);
        userReg.setCredentials(creds);

        return userReg;
    }

    private void testRegisterUserSuccess() {
        UserRegistration userReg = createUserRegistration();
        makeSureUserNoExists(userReg.getCredentials().getUsername());

        String json = restTemplate.postForObject(getRestAPIHostPort() + "/pls/users", userReg, String.class);
        ResponseDocument<RegistrationResult> response = ResponseDocument.generateFromJSON(json, RegistrationResult.class);
        assertTrue(response.isSuccess());
        assertNotNull(response.getResult().getPassword());

        User newUser = globalUserManagementService.getUserByEmail(userReg.getUser().getEmail());
        assertNotNull(newUser);
        assertEquals(newUser.getFirstName(), userReg.getUser().getFirstName());
        assertEquals(newUser.getLastName(), userReg.getUser().getLastName());
        assertEquals(newUser.getPhoneNumber(), userReg.getUser().getPhoneNumber());
        assertEquals(newUser.getTitle(), userReg.getUser().getTitle());

        String password = response.getResult().getPassword();
        Ticket ticket = globalAuthenticationService.authenticateUser(userReg.getUser().getUsername(), DigestUtils.sha256Hex(password));
        assertEquals(ticket.getTenants().size(), 1);
        globalAuthenticationService.discard(ticket);

        makeSureUserNoExists(userReg.getCredentials().getUsername());
    }

    private void testRegisterUserFail() {
        UserRegistration userReg = createUserRegistration();
        makeSureUserNoExists(userReg.getCredentials().getUsername());

        boolean exception = false;
        try {
            restTemplate.postForObject(getRestAPIHostPort() + "/pls/users", userReg, String.class);
        } catch (HttpClientErrorException e) {
            exception = true;
            assertEquals(e.getStatusCode().value(), 403);
        }
        assertTrue(exception);
        assertNull(globalUserManagementService.getUserByEmail(userReg.getUser().getEmail()));

        makeSureUserNoExists(userReg.getCredentials().getUsername());
    }

    private void testUpdateAccessLevel(AccessLevel targetLevel, boolean expectSuccess) {
        if (expectSuccess)
            updateAccessLevelWithSufficientPrivilege(targetLevel);
        else
            updateAccessLevelWithoutSufficientPrivilege(targetLevel);
    }

    private void updateAccessLevelWithSufficientPrivilege(AccessLevel accessLevel) {
        User user = createTestUser(AccessLevel.EXTERNAL_USER);

        UserUpdateData data = new UserUpdateData();
        data.setAccessLevel(accessLevel.name());

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>(data.toString(), headers);

        String url = getRestAPIHostPort() + "/pls/users/" + user.getUsername();
        ResponseEntity<ResponseDocument> response = restTemplate.exchange(url, HttpMethod.PUT, requestEntity, ResponseDocument.class);
        assertTrue(response.getBody().isSuccess());

        AccessLevel resultLevel = userService.getAccessLevel(testTenant.getId(), user.getUsername());
        assertEquals(accessLevel, resultLevel);

        makeSureUserNoExists(user.getUsername());
    }

    private void updateAccessLevelWithoutSufficientPrivilege(AccessLevel accessLevel) {
        User user = createTestUser(AccessLevel.EXTERNAL_USER);

        UserUpdateData data = new UserUpdateData();
        data.setAccessLevel(accessLevel.name());

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>(data.toString(), headers);

        String url = getRestAPIHostPort() + "/pls/users/" + user.getUsername();

        try {
            restTemplate.exchange(url, HttpMethod.PUT, requestEntity, ResponseDocument.class);
        } catch (HttpClientErrorException e) {
            assertEquals(e.getStatusCode().value(), 403);
        }
    }

    private void testConflictingUserInTenant() {
        User existingUser = createTestUser(AccessLevel.EXTERNAL_USER);

        UserRegistration uReg = createUserRegistration();
        uReg.getUser().setEmail(existingUser.getEmail());

        String json = restTemplate.postForObject(getRestAPIHostPort() + "/pls/users", uReg, String.class);
        ResponseDocument<RegistrationResult> response = ResponseDocument.generateFromJSON(json, RegistrationResult.class);
        assertFalse(response.getResult().isValid());
        // when conflict with another use in the same tenant, do not show the conflicting user
        assertNull(response.getResult().getConflictingUser());
    }

    private void testConflictingUserOutsideTenant(
            AccessLevel existingUserAccessLevel, AccessLevel testUserAccessLevel) {
        User existingUser = createTestUser(existingUserAccessLevel);
        userService.resignAccessLevel(testTenant.getId(), existingUser.getUsername());

        UserRegistration uReg = createUserRegistration();
        uReg.getUser().setEmail(existingUser.getEmail());

        String json = restTemplate.postForObject(getRestAPIHostPort() + "/pls/users", uReg, String.class);
        ResponseDocument<RegistrationResult> response = ResponseDocument.generateFromJSON(json, RegistrationResult.class);
        assertFalse(response.getResult().isValid());
        // when conflict with another use outside the same tenant,
        // show the conflicting user if its access level is lower than the current logged in user
        User user =  response.getResult().getConflictingUser();
        if (testUserAccessLevel.compareTo(existingUserAccessLevel) >= 0) {
            assertEquals(user.getFirstName(), existingUser.getFirstName());
            assertEquals(user.getLastName(), existingUser.getLastName());
            assertEquals(user.getEmail(), existingUser.getEmail());
            assertEquals(user.getUsername(), existingUser.getUsername());
        } else {
            assertNull(user);
        }
    }

    private void testGetAllUsersFail() {
        boolean exception = false;
        try {
            restTemplate.getForObject(getRestAPIHostPort() + "/pls/users", String.class);
        } catch (HttpClientErrorException e) {
            exception = true;
            assertEquals(e.getStatusCode().value(), 403);
        }
        assertTrue(exception);
    }

    private void testGetAllUsersSuccess(int expectedNumOfVisibleUsers) {
        String json = restTemplate.getForObject(getRestAPIHostPort() + "/pls/users", String.class);
        ResponseDocument<ArrayNode> response =  ResponseDocument.generateFromJSON(json, ArrayNode.class);
        ArrayNode users = response.getResult();
        assertEquals(users.size(), expectedNumOfVisibleUsers);
    }

    protected void testChangePassword(AccessLevel accessLevel) {
        User user = createTestUser(accessLevel);
        UserDocument doc = loginAndAttach(user.getUsername());
        useSessionDoc(doc);

        UserUpdateData data = new UserUpdateData();
        data.setOldPassword(DigestUtils.sha256Hex("wrong"));
        data.setNewPassword(DigestUtils.sha256Hex("newpass"));

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>(data.toString(), headers);

        String url = getRestAPIHostPort() + "/pls/users/" + user.getUsername() + "/creds";
        ResponseEntity<ResponseDocument> response = restTemplate.exchange(url, HttpMethod.PUT, requestEntity, ResponseDocument.class);
        assertFalse(response.getBody().isSuccess());

        data.setOldPassword(DigestUtils.sha256Hex(generalPassword));
        requestEntity = new HttpEntity<>(data.toString(), headers);
        response = restTemplate.exchange(url, HttpMethod.PUT, requestEntity, ResponseDocument.class);
        assertEquals(response.getStatusCode().value(), 200);
        assertTrue(response.getBody().isSuccess());

        logoutUserDoc(doc);

        Ticket ticket = loginCreds(user.getUsername(), "newpass");
        logoutTicket(ticket);

        makeSureUserNoExists(user.getUsername());
    }
}
