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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.UserService;


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
        testRegisterUserSuccess(AccessLevel.SUPER_ADMIN);

        switchToAccessLevel(AccessLevel.INTERNAL_ADMIN);
        testRegisterUserSuccess(AccessLevel.INTERNAL_ADMIN);
        testRegisterUserFail(AccessLevel.SUPER_ADMIN);

        switchToAccessLevel(AccessLevel.INTERNAL_USER);
        testRegisterUserFail(AccessLevel.EXTERNAL_USER);
        testRegisterUserFail(AccessLevel.INTERNAL_USER);

        switchToAccessLevel(AccessLevel.EXTERNAL_ADMIN);
        testRegisterUserSuccess(AccessLevel.EXTERNAL_ADMIN);
        testRegisterUserFail(AccessLevel.INTERNAL_USER);

        switchToAccessLevel(AccessLevel.EXTERNAL_USER);
        testRegisterUserFail(AccessLevel.EXTERNAL_USER);
    }

    @Test(groups = { "functional", "deployment" })
    public void validateNewUser() {
        // Conflict with a user in the same tenant
        testConflictingUserInTenant();
        testConflictingUserOutsideTenant();
    }

    @Test(groups = { "functional", "deployment" })
    public void getAllUsers() {
        switchToAccessLevel(AccessLevel.EXTERNAL_USER);
        testGetAllUsersFail();

        switchToAccessLevel(AccessLevel.INTERNAL_USER);
        testGetAllUsersFail();

        switchToAccessLevel(AccessLevel.EXTERNAL_ADMIN);
        testGetAllUsersSuccess(2);

        switchToAccessLevel(AccessLevel.INTERNAL_ADMIN);
        testGetAllUsersSuccess(5);

        switchToAccessLevel(AccessLevel.SUPER_ADMIN);
        testGetAllUsersSuccess(5);
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
    public void updateAccessLevel() {
        User user = createTestUser(AccessLevel.EXTERNAL_USER);
        testUpdateAccessLevel(user, AccessLevel.EXTERNAL_USER, true);
        testUpdateAccessLevel(user, AccessLevel.EXTERNAL_ADMIN, true);
        testUpdateAccessLevel(user, AccessLevel.INTERNAL_USER, true);
        testUpdateAccessLevel(user, AccessLevel.INTERNAL_ADMIN, true);
        testUpdateAccessLevel(user, AccessLevel.SUPER_ADMIN, true);

        userService.assignAccessLevel(AccessLevel.EXTERNAL_USER, testTenant.getId(), user.getUsername());
        switchToAccessLevel(AccessLevel.INTERNAL_ADMIN);
        testUpdateAccessLevel(user, AccessLevel.EXTERNAL_USER, true);
        testUpdateAccessLevel(user, AccessLevel.EXTERNAL_ADMIN, true);
        testUpdateAccessLevel(user, AccessLevel.INTERNAL_USER, true);
        testUpdateAccessLevel(user, AccessLevel.INTERNAL_ADMIN, true);
        testUpdateAccessLevel(user, AccessLevel.SUPER_ADMIN, false);

        userService.assignAccessLevel(AccessLevel.EXTERNAL_USER, testTenant.getId(), user.getUsername());
        switchToAccessLevel(AccessLevel.INTERNAL_USER);
        testUpdateAccessLevel(user, AccessLevel.EXTERNAL_USER, false);
        testUpdateAccessLevel(user, AccessLevel.EXTERNAL_ADMIN, false);
        testUpdateAccessLevel(user, AccessLevel.INTERNAL_USER, false);
        testUpdateAccessLevel(user, AccessLevel.INTERNAL_ADMIN, false);
        testUpdateAccessLevel(user, AccessLevel.SUPER_ADMIN, false);

        userService.assignAccessLevel(AccessLevel.EXTERNAL_USER, testTenant.getId(), user.getUsername());
        switchToAccessLevel(AccessLevel.EXTERNAL_ADMIN);
        testUpdateAccessLevel(user, AccessLevel.EXTERNAL_USER, true);
        testUpdateAccessLevel(user, AccessLevel.EXTERNAL_ADMIN, true);
        testUpdateAccessLevel(user, AccessLevel.INTERNAL_USER, false);
        testUpdateAccessLevel(user, AccessLevel.INTERNAL_ADMIN, false);
        testUpdateAccessLevel(user, AccessLevel.SUPER_ADMIN, false);

        userService.assignAccessLevel(AccessLevel.EXTERNAL_USER, testTenant.getId(), user.getUsername());
        switchToAccessLevel(AccessLevel.EXTERNAL_USER);
        testUpdateAccessLevel(user, AccessLevel.EXTERNAL_USER, false);
        testUpdateAccessLevel(user, AccessLevel.EXTERNAL_ADMIN, false);
        testUpdateAccessLevel(user, AccessLevel.INTERNAL_USER, false);
        testUpdateAccessLevel(user, AccessLevel.INTERNAL_ADMIN, false);
        testUpdateAccessLevel(user, AccessLevel.SUPER_ADMIN, false);

        makeSureUserDoesNotExist(user.getUsername());
    }

    @Test(groups = { "functional", "deployment" })
    public void deleteUser() {
        switchToAccessLevel(AccessLevel.SUPER_ADMIN);
        testDeleteUserSuccess(AccessLevel.EXTERNAL_USER);
    }


    @SuppressWarnings("rawtypes")
	@Test(groups = { "functional", "deployment" })
    public void deleteUserWithShortEmail() {
        String shortEmail = "a@b.c";
        makeSureUserDoesNotExist(shortEmail);
        createUser(shortEmail, shortEmail, "Short", "Email");
        userService.assignAccessLevel(AccessLevel.INTERNAL_USER, testTenant.getId(), shortEmail);

        HttpHeaders headers = new HttpHeaders();
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>("", headers);

        String url = getRestAPIHostPort() + "/pls/users/\"" + shortEmail + "\"";
        ResponseEntity<ResponseDocument> response = restTemplate.exchange(url, HttpMethod.DELETE, requestEntity,
                ResponseDocument.class);
        assertTrue(response.getBody().isSuccess());

        makeSureUserDoesNotExist(shortEmail);
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

        String json = restTemplate.postForObject(getRestAPIHostPort() + "/pls/users", userReg, String.class);
        ResponseDocument<RegistrationResult> response = ResponseDocument.generateFromJSON(json, RegistrationResult.class);
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
        Ticket ticket = globalAuthenticationService.authenticateUser(userReg.getUser().getUsername(), DigestUtils.sha256Hex(password));
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
            restTemplate.postForObject(getRestAPIHostPort() + "/pls/users", userReg, String.class);
        } catch (RuntimeException e) {
            exception = true;
            assertEquals(e.getMessage(), "403");
        }
        assertTrue(exception);
        assertNull(globalUserManagementService.getUserByEmail(userReg.getUser().getEmail()));

        makeSureUserDoesNotExist(userReg.getCredentials().getUsername());
    }

    @SuppressWarnings("rawtypes")
	private void testDeleteUserSuccess(AccessLevel accessLevel) {
        User user = createTestUser(accessLevel);

        HttpHeaders headers = new HttpHeaders();
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>("", headers);

        String url = getRestAPIHostPort() + "/pls/users/" + user.getUsername();
        ResponseEntity<ResponseDocument> response = restTemplate.exchange(url, HttpMethod.DELETE, requestEntity,
                ResponseDocument.class);
        assertTrue(response.getBody().isSuccess());

        makeSureUserDoesNotExist(user.getUsername());
    }


    private void testUpdateAccessLevel(User user, AccessLevel targetLevel, boolean expectSuccess) {
        if (expectSuccess)
            updateAccessLevelWithSufficientPrivilege(user, targetLevel);
        else
            updateAccessLevelWithoutSufficientPrivilege(user, targetLevel);
    }

    @SuppressWarnings("rawtypes")
    private void updateAccessLevelWithSufficientPrivilege(User user, AccessLevel accessLevel) {
        UserUpdateData data = new UserUpdateData();
        data.setAccessLevel(accessLevel.name());

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>(data.toString(), headers);

        String url = getRestAPIHostPort() + "/pls/users/" + user.getUsername();
        ResponseEntity<ResponseDocument> response = restTemplate.exchange(url, HttpMethod.PUT, requestEntity,
                ResponseDocument.class);
        assertTrue(response.getBody().isSuccess());

        AccessLevel resultLevel = userService.getAccessLevel(testTenant.getId(), user.getUsername());
        assertEquals(accessLevel, resultLevel);
    }

    private void updateAccessLevelWithoutSufficientPrivilege(User user, AccessLevel accessLevel) {
        UserUpdateData data = new UserUpdateData();
        data.setAccessLevel(accessLevel.name());

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>(data.toString(), headers);

        String url = getRestAPIHostPort() + "/pls/users/" + user.getUsername();

        boolean exception = false;
        try {
            restTemplate.exchange(url, HttpMethod.PUT, requestEntity, ResponseDocument.class);
        } catch (RuntimeException e) {
            exception = true;
            assertEquals(e.getMessage(), "403");
        }
        assertTrue(exception);
    }

    private void testConflictingUserInTenant() {
        User existingUser = createTestUser(AccessLevel.EXTERNAL_USER);

        UserRegistration uReg = createUserRegistration();
        uReg.getUser().setEmail(existingUser.getEmail());

        String json = restTemplate.postForObject(getRestAPIHostPort() + "/pls/users", uReg, String.class);
        ResponseDocument<RegistrationResult> response = ResponseDocument.generateFromJSON(json, RegistrationResult.class);
        assertNotNull(response);
        assertFalse(response.getResult().isValid());
        assertNull(response.getResult().getConflictingUser(),
                "When conflict with another use in the same tenant, should not show the conflicting user");

        makeSureUserDoesNotExist(existingUser.getUsername());
    }

    private void testConflictingUserOutsideTenant() {
        User existingUser = createTestUser(AccessLevel.EXTERNAL_USER);
        userService.resignAccessLevel(testTenant.getId(), existingUser.getUsername());

        UserRegistration uReg = createUserRegistration();
        uReg.getUser().setEmail(existingUser.getEmail());

        String json = restTemplate.postForObject(getRestAPIHostPort() + "/pls/users", uReg, String.class);
        ResponseDocument<RegistrationResult> response = ResponseDocument.generateFromJSON(json, RegistrationResult.class);
        assertNotNull(response);
        assertFalse(response.getResult().isValid());
        // when conflict with another use outside the same tenant,
        // show the conflicting user if its access level is lower than the current logged in user
        User user =  response.getResult().getConflictingUser();
        assertEquals(user.getFirstName(), existingUser.getFirstName());
        assertEquals(user.getLastName(), existingUser.getLastName());
        assertEquals(user.getEmail(), existingUser.getEmail());
        assertEquals(user.getUsername(), existingUser.getUsername());

        makeSureUserDoesNotExist(existingUser.getUsername());
    }

    private void testGetAllUsersFail() {
        boolean exception = false;
        try {
            restTemplate.getForObject(getRestAPIHostPort() + "/pls/users", String.class);
        } catch (RuntimeException e) {
            exception = true;
            assertEquals(e.getMessage(), "403");
        }
        assertTrue(exception);
    }

    private void testGetAllUsersSuccess(int expectedNumOfVisibleUsers) {
        String json = restTemplate.getForObject(getRestAPIHostPort() + "/pls/users", String.class);
        ResponseDocument<ArrayNode> response =  ResponseDocument.generateFromJSON(json, ArrayNode.class);
        assertNotNull(response);
        ArrayNode users = response.getResult();
        assertEquals(users.size(), expectedNumOfVisibleUsers);
    }

    @SuppressWarnings("rawtypes")
    private void testChangePassword(AccessLevel accessLevel) {
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

        String url = getRestAPIHostPort() + "/pls/users/creds";

        boolean exception = false;
        try {
            restTemplate.exchange(url, HttpMethod.PUT, requestEntity, ResponseDocument.class);
        } catch (RuntimeException e) {
            exception = true;
            assertEquals(e.getMessage(), "401");
        }
        assertTrue(exception);

        data.setOldPassword(DigestUtils.sha256Hex(generalPassword));
        requestEntity = new HttpEntity<>(data.toString(), headers);
        ResponseEntity<ResponseDocument> response = restTemplate.exchange(url, HttpMethod.PUT, requestEntity, ResponseDocument.class);
        assertEquals(response.getStatusCode().value(), 200);
        assertTrue(response.getBody().isSuccess());

        logoutUserDoc(doc);

        Ticket ticket = loginCreds(user.getUsername(), "newpass");
        logoutTicket(ticket);

        makeSureUserDoesNotExist(user.getUsername());
    }
}
