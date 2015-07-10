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
        this.createTestTenant();
        this.createTestUsers();
    }

    @AfterClass(groups = { "functional", "deployment" })
    public void tearDown() {
        this.destroyTestUsers();
        this.destroyTestTenant();
    }

    @BeforeMethod(groups = { "functional", "deployment" })
    public void beforeMethod() {
        this.switchToAccessLevel(AccessLevel.SUPER_ADMIN);
    }

    @Test(groups = { "functional", "deployment" })
    public void registerUser() {
        this.switchToAccessLevel(AccessLevel.SUPER_ADMIN);
        this.testRegisterUserSuccess(AccessLevel.SUPER_ADMIN);

        this.switchToAccessLevel(AccessLevel.INTERNAL_ADMIN);
        this.testRegisterUserSuccess(AccessLevel.INTERNAL_ADMIN);
        this.testRegisterUserFail(AccessLevel.SUPER_ADMIN);

        this.switchToAccessLevel(AccessLevel.INTERNAL_USER);
        this.testRegisterUserFail(AccessLevel.EXTERNAL_USER);
        this.testRegisterUserFail(AccessLevel.INTERNAL_USER);

        this.switchToAccessLevel(AccessLevel.EXTERNAL_ADMIN);
        this.testRegisterUserSuccess(AccessLevel.EXTERNAL_ADMIN);
        this.testRegisterUserFail(AccessLevel.INTERNAL_USER);

        this.switchToAccessLevel(AccessLevel.EXTERNAL_USER);
        this.testRegisterUserFail(AccessLevel.EXTERNAL_USER);
    }

    @Test(groups = { "functional", "deployment" })
    public void validateNewUser() {
        // Conflict with a user in the same tenant
        this.testConflictingUserInTenant();
        this.testConflictingUserOutsideTenant();
    }

    @Test(groups = { "functional", "deployment" })
    public void getAllUsers() {
        this.switchToAccessLevel(AccessLevel.EXTERNAL_USER);
        this.testGetAllUsersFail();

        this.switchToAccessLevel(AccessLevel.INTERNAL_USER);
        this.testGetAllUsersFail();

        this.switchToAccessLevel(AccessLevel.EXTERNAL_ADMIN);
        this.testGetAllUsersSuccess(2);

        this.switchToAccessLevel(AccessLevel.INTERNAL_ADMIN);
        this.testGetAllUsersSuccess(5);

        this.switchToAccessLevel(AccessLevel.SUPER_ADMIN);
        this.testGetAllUsersSuccess(5);
    }

    @Test(groups = { "functional", "deployment" })
    public void changePassword() {
        this.testChangePassword(AccessLevel.EXTERNAL_USER);
        this.testChangePassword(AccessLevel.EXTERNAL_ADMIN);
        this.testChangePassword(AccessLevel.INTERNAL_USER);
        this.testChangePassword(AccessLevel.INTERNAL_ADMIN);
        this.testChangePassword(AccessLevel.SUPER_ADMIN);
    }

    @Test(groups = { "functional", "deployment" })
    public void updateAccessLevel() {
        User user = this.createTestUser(AccessLevel.EXTERNAL_USER);
        this.testUpdateAccessLevel(user, AccessLevel.EXTERNAL_USER, true);
        this.testUpdateAccessLevel(user, AccessLevel.EXTERNAL_ADMIN, true);
        this.testUpdateAccessLevel(user, AccessLevel.INTERNAL_USER, true);
        this.testUpdateAccessLevel(user, AccessLevel.INTERNAL_ADMIN, true);
        this.testUpdateAccessLevel(user, AccessLevel.SUPER_ADMIN, true);

        this.userService.assignAccessLevel(AccessLevel.EXTERNAL_USER, this.testTenant.getId(), user.getUsername());
        this.switchToAccessLevel(AccessLevel.INTERNAL_ADMIN);
        this.testUpdateAccessLevel(user, AccessLevel.EXTERNAL_USER, true);
        this.testUpdateAccessLevel(user, AccessLevel.EXTERNAL_ADMIN, true);
        this.testUpdateAccessLevel(user, AccessLevel.INTERNAL_USER, true);
        this.testUpdateAccessLevel(user, AccessLevel.INTERNAL_ADMIN, true);
        this.testUpdateAccessLevel(user, AccessLevel.SUPER_ADMIN, false);

        this.userService.assignAccessLevel(AccessLevel.EXTERNAL_USER, this.testTenant.getId(), user.getUsername());
        this.switchToAccessLevel(AccessLevel.INTERNAL_USER);
        this.testUpdateAccessLevel(user, AccessLevel.EXTERNAL_USER, false);
        this.testUpdateAccessLevel(user, AccessLevel.EXTERNAL_ADMIN, false);
        this.testUpdateAccessLevel(user, AccessLevel.INTERNAL_USER, false);
        this.testUpdateAccessLevel(user, AccessLevel.INTERNAL_ADMIN, false);
        this.testUpdateAccessLevel(user, AccessLevel.SUPER_ADMIN, false);

        this.userService.assignAccessLevel(AccessLevel.EXTERNAL_USER, this.testTenant.getId(), user.getUsername());
        this.switchToAccessLevel(AccessLevel.EXTERNAL_ADMIN);
        this.testUpdateAccessLevel(user, AccessLevel.EXTERNAL_USER, true);
        this.testUpdateAccessLevel(user, AccessLevel.EXTERNAL_ADMIN, true);
        this.testUpdateAccessLevel(user, AccessLevel.INTERNAL_USER, false);
        this.testUpdateAccessLevel(user, AccessLevel.INTERNAL_ADMIN, false);
        this.testUpdateAccessLevel(user, AccessLevel.SUPER_ADMIN, false);

        this.userService.assignAccessLevel(AccessLevel.EXTERNAL_USER, this.testTenant.getId(), user.getUsername());
        this.switchToAccessLevel(AccessLevel.EXTERNAL_USER);
        this.testUpdateAccessLevel(user, AccessLevel.EXTERNAL_USER, false);
        this.testUpdateAccessLevel(user, AccessLevel.EXTERNAL_ADMIN, false);
        this.testUpdateAccessLevel(user, AccessLevel.INTERNAL_USER, false);
        this.testUpdateAccessLevel(user, AccessLevel.INTERNAL_ADMIN, false);
        this.testUpdateAccessLevel(user, AccessLevel.SUPER_ADMIN, false);

        this.makeSureUserDoesNotExist(user.getUsername());
    }

    @Test(groups = { "functional", "deployment" })
    public void deleteUser() {
        this.switchToAccessLevel(AccessLevel.SUPER_ADMIN);
        this.testDeleteUserSuccess(AccessLevel.EXTERNAL_USER);
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = { "functional", "deployment" })
    public void deleteUserWithShortEmail() {
        String shortEmail = "a@b.c";
        this.makeSureUserDoesNotExist(shortEmail);
        this.createUser(shortEmail, shortEmail, "Short", "Email");
        this.userService.assignAccessLevel(AccessLevel.INTERNAL_USER, this.testTenant.getId(), shortEmail);

        HttpHeaders headers = new HttpHeaders();
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>("", headers);

        String url = this.getRestAPIHostPort() + "/pls/users/\"" + shortEmail + "\"";
        ResponseEntity<ResponseDocument> response = this.restTemplate.exchange(url, HttpMethod.DELETE, requestEntity,
                ResponseDocument.class);
        assertTrue(response.getBody().isSuccess());

        this.makeSureUserDoesNotExist(shortEmail);
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
        UserRegistration userReg = this.createUserRegistration();
        userReg.getUser().setAccessLevel(accessLevel.name());
        this.makeSureUserDoesNotExist(userReg.getCredentials().getUsername());

        String json = this.restTemplate.postForObject(this.getRestAPIHostPort() + "/pls/users", userReg, String.class);
        ResponseDocument<RegistrationResult> response = ResponseDocument.generateFromJSON(json,
                RegistrationResult.class);
        assertNotNull(response);
        assertTrue(response.isSuccess());
        assertNotNull(response.getResult().getPassword());

        User newUser = this.userService.findByEmail(userReg.getUser().getEmail());
        assertNotNull(newUser);
        assertEquals(newUser.getFirstName(), userReg.getUser().getFirstName());
        assertEquals(newUser.getLastName(), userReg.getUser().getLastName());
        assertEquals(newUser.getPhoneNumber(), userReg.getUser().getPhoneNumber());
        assertEquals(newUser.getTitle(), userReg.getUser().getTitle());

        String password = response.getResult().getPassword();
        Ticket ticket = this.globalAuthenticationService.authenticateUser(userReg.getUser().getUsername(),
                DigestUtils.sha256Hex(password));
        assertEquals(ticket.getTenants().size(), 1);
        this.globalAuthenticationService.discard(ticket);

        this.makeSureUserDoesNotExist(userReg.getCredentials().getUsername());
    }

    private void testRegisterUserFail(AccessLevel accessLevel) {
        UserRegistration userReg = this.createUserRegistration();
        userReg.getUser().setAccessLevel(accessLevel.name());
        this.makeSureUserDoesNotExist(userReg.getCredentials().getUsername());

        boolean exception = false;
        try {
            this.restTemplate.postForObject(this.getRestAPIHostPort() + "/pls/users", userReg, String.class);
        } catch (RuntimeException e) {
            exception = true;
            assertEquals(e.getMessage(), "403");
        }
        assertTrue(exception);
        assertNull(this.globalUserManagementService.getUserByEmail(userReg.getUser().getEmail()));

        this.makeSureUserDoesNotExist(userReg.getCredentials().getUsername());
    }

    @SuppressWarnings("rawtypes")
    private void testDeleteUserSuccess(AccessLevel accessLevel) {
        User user = this.createTestUser(accessLevel);

        HttpHeaders headers = new HttpHeaders();
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>("", headers);

        String url = this.getRestAPIHostPort() + "/pls/users/" + user.getUsername();
        ResponseEntity<ResponseDocument> response = this.restTemplate.exchange(url, HttpMethod.DELETE, requestEntity,
                ResponseDocument.class);
        assertTrue(response.getBody().isSuccess());

        this.makeSureUserDoesNotExist(user.getUsername());
    }

    private void testUpdateAccessLevel(User user, AccessLevel targetLevel, boolean expectSuccess) {
        if (expectSuccess) {
            this.updateAccessLevelWithSufficientPrivilege(user, targetLevel);
        } else {
            this.updateAccessLevelWithoutSufficientPrivilege(user, targetLevel);
        }
    }

    @SuppressWarnings("rawtypes")
    private void updateAccessLevelWithSufficientPrivilege(User user, AccessLevel accessLevel) {
        UserUpdateData data = new UserUpdateData();
        data.setAccessLevel(accessLevel.name());

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>(data.toString(), headers);

        String url = this.getRestAPIHostPort() + "/pls/users/" + user.getUsername();
        ResponseEntity<ResponseDocument> response = this.restTemplate.exchange(url, HttpMethod.PUT, requestEntity,
                ResponseDocument.class);
        assertTrue(response.getBody().isSuccess());

        AccessLevel resultLevel = this.userService.getAccessLevel(this.testTenant.getId(), user.getUsername());
        assertEquals(accessLevel, resultLevel);
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = { "functional", "deployment" })
    public void stringifiedUserName_updateAccessLevel_acessLevelSuccessfullyUpdated() {
        User user = this.createTestUser(AccessLevel.EXTERNAL_USER);
        UserUpdateData data = new UserUpdateData();
        data.setAccessLevel(user.getAccessLevel());

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>(data.toString(), headers);

        String url = this.getRestAPIHostPort() + "/pls/users/\"" + user.getUsername() + "\"";
        ResponseEntity<ResponseDocument> response = this.restTemplate.exchange(url, HttpMethod.PUT, requestEntity,
                ResponseDocument.class);
        assertTrue(response.getBody().isSuccess());
    }

    private void updateAccessLevelWithoutSufficientPrivilege(User user, AccessLevel accessLevel) {
        UserUpdateData data = new UserUpdateData();
        data.setAccessLevel(accessLevel.name());

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>(data.toString(), headers);

        String url = this.getRestAPIHostPort() + "/pls/users/" + user.getUsername();

        boolean exception = false;
        try {
            this.restTemplate.exchange(url, HttpMethod.PUT, requestEntity, ResponseDocument.class);
        } catch (RuntimeException e) {
            exception = true;
            assertEquals(e.getMessage(), "403");
        }
        assertTrue(exception);
    }

    private void testConflictingUserInTenant() {
        User existingUser = this.createTestUser(AccessLevel.EXTERNAL_USER);

        UserRegistration uReg = this.createUserRegistration();
        uReg.getUser().setEmail(existingUser.getEmail());

        String json = this.restTemplate.postForObject(this.getRestAPIHostPort() + "/pls/users", uReg, String.class);
        ResponseDocument<RegistrationResult> response = ResponseDocument.generateFromJSON(json,
                RegistrationResult.class);
        assertNotNull(response);
        assertFalse(response.getResult().isValid());
        assertNull(response.getResult().getConflictingUser(),
                "When conflict with another use in the same tenant, should not show the conflicting user");

        this.makeSureUserDoesNotExist(existingUser.getUsername());
    }

    private void testConflictingUserOutsideTenant() {
        User existingUser = this.createTestUser(AccessLevel.EXTERNAL_USER);
        this.userService.resignAccessLevel(this.testTenant.getId(), existingUser.getUsername());

        UserRegistration uReg = this.createUserRegistration();
        uReg.getUser().setEmail(existingUser.getEmail());

        String json = this.restTemplate.postForObject(this.getRestAPIHostPort() + "/pls/users", uReg, String.class);
        ResponseDocument<RegistrationResult> response = ResponseDocument.generateFromJSON(json,
                RegistrationResult.class);
        assertNotNull(response);
        assertFalse(response.getResult().isValid());
        // when conflict with another use outside the same tenant,
        // show the conflicting user if its access level is lower than the
        // current logged in user
        User user = response.getResult().getConflictingUser();
        assertEquals(user.getFirstName(), existingUser.getFirstName());
        assertEquals(user.getLastName(), existingUser.getLastName());
        assertEquals(user.getEmail(), existingUser.getEmail());
        assertEquals(user.getUsername(), existingUser.getUsername());

        this.makeSureUserDoesNotExist(existingUser.getUsername());
    }

    private void testGetAllUsersFail() {
        boolean exception = false;
        try {
            this.restTemplate.getForObject(this.getRestAPIHostPort() + "/pls/users", String.class);
        } catch (RuntimeException e) {
            exception = true;
            assertEquals(e.getMessage(), "403");
        }
        assertTrue(exception);
    }

    private void testGetAllUsersSuccess(int expectedNumOfVisibleUsers) {
        String json = this.restTemplate.getForObject(this.getRestAPIHostPort() + "/pls/users", String.class);
        ResponseDocument<ArrayNode> response = ResponseDocument.generateFromJSON(json, ArrayNode.class);
        assertNotNull(response);
        ArrayNode users = response.getResult();
        assertEquals(users.size(), expectedNumOfVisibleUsers);
    }

    @SuppressWarnings("rawtypes")
    private void testChangePassword(AccessLevel accessLevel) {
        User user = this.createTestUser(accessLevel);
        UserDocument doc = this.loginAndAttach(user.getUsername());
        this.useSessionDoc(doc);

        UserUpdateData data = new UserUpdateData();
        data.setOldPassword(DigestUtils.sha256Hex("wrong"));
        data.setNewPassword(DigestUtils.sha256Hex("newpass"));

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>(data.toString(), headers);

        String url = this.getRestAPIHostPort() + "/pls/users/creds";

        boolean exception = false;
        try {
            this.restTemplate.exchange(url, HttpMethod.PUT, requestEntity, ResponseDocument.class);
        } catch (RuntimeException e) {
            exception = true;
            assertEquals(e.getMessage(), "401");
        }
        assertTrue(exception);

        data.setOldPassword(DigestUtils.sha256Hex(generalPassword));
        requestEntity = new HttpEntity<>(data.toString(), headers);
        ResponseEntity<ResponseDocument> response = this.restTemplate.exchange(url, HttpMethod.PUT, requestEntity,
                ResponseDocument.class);
        assertEquals(response.getStatusCode().value(), 200);
        assertTrue(response.getBody().isSuccess());

        this.logoutUserDoc(doc);

        Ticket ticket = this.loginCreds(user.getUsername(), "newpass");
        this.logoutTicket(ticket);

        this.makeSureUserDoesNotExist(user.getUsername());
    }
}
