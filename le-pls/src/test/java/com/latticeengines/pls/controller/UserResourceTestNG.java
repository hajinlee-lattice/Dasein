package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.HttpClientErrorException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.latticeengines.domain.exposed.pls.DeleteUsersResult;
import com.latticeengines.domain.exposed.pls.RegistrationResult;
import com.latticeengines.domain.exposed.pls.ResponseDocument;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.pls.UserUpdateData;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.EntityAccessRightsData;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.globalauth.authentication.GlobalAuthenticationService;
import com.latticeengines.pls.globalauth.authentication.GlobalSessionManagementService;
import com.latticeengines.pls.globalauth.authentication.GlobalTenantManagementService;
import com.latticeengines.pls.globalauth.authentication.GlobalUserManagementService;
import com.latticeengines.pls.security.AccessLevel;
import com.latticeengines.pls.security.GrantedRight;


public class UserResourceTestNG extends PlsFunctionalTestNGBase {
    @SuppressWarnings("unused")
    private static Log log = LogFactory.getLog(UserResourceTestNG.class);

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @Autowired
    private GlobalTenantManagementService globalTenantManagementService;

    @Autowired
    private GlobalSessionManagementService globalSessionManagementService;

    private Tenant testTenant;
    private User testUser;
    private Credentials testCreds;

    private UserDocument adminDoc;
    private UserDocument generalDoc;

    @BeforeClass(groups = { "functional", "deployment" })
    public void setup() throws Exception {
        if (!usersInitialized) { setupUsers(); }

        adminDoc = loginAndAttachAdmin();
        generalDoc = loginAndAttachGeneral();
        testTenant = globalSessionManagementService.retrieve(adminDoc.getTicket()).getTenant();

        UserRegistration testReg = createUserRegistration();
        testUser = testReg.getUser();
        testCreds = testReg.getCredentials();

        globalUserManagementService.deleteUser(testCreds.getUsername());

        setupDb(testTenant.getId(), null);

        createUser(testUser.getUsername(), testUser.getEmail(), testUser.getFirstName(), testUser.getLastName());
        grantDefaultRights(testTenant.getId(), testUser.getUsername());
    }

    @AfterClass(groups = { "functional", "deployment" })
    public void tearDown() {
        globalUserManagementService.deleteUser(testCreds.getUsername());
        globalAuthenticationService.discard(adminDoc.getTicket());
    }

    @BeforeMethod(groups = { "functional", "deployment" })
    public void beforeMethod() {
        // using admin session by default
        addAuthHeader.setAuthValue(adminDoc.getTicket().getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addAuthHeader}));
    }

    @Test(groups = { "functional", "deployment" })
    public void registerUser() {
        UserRegistration userReg = createUserRegistration();
        assertTrue(globalUserManagementService.deleteUser(userReg.getCredentials().getUsername()));

        String json = restTemplate.postForObject(getRestAPIHostPort() + "/pls/users", userReg, String.class);
        ResponseDocument<RegistrationResult> response = ResponseDocument.generateFromJSON(json, RegistrationResult.class);
        assertTrue(response.isSuccess());
        assertNotNull(response.getResult().getPassword());

        User newUser = globalUserManagementService.getUserByEmail(userReg.getUser().getEmail());
        assertEquals(newUser.getFirstName(), userReg.getUser().getFirstName());
        assertEquals(newUser.getLastName(), userReg.getUser().getLastName());
        assertEquals(newUser.getPhoneNumber(), userReg.getUser().getPhoneNumber());
        assertEquals(newUser.getTitle(), userReg.getUser().getTitle());

        // try log in new user
        String password = response.getResult().getPassword();
        Ticket ticket = globalAuthenticationService.authenticateUser(userReg.getUser().getUsername(), DigestUtils.sha256Hex(password));
        assertEquals(ticket.getTenants().size(), 1);
        globalAuthenticationService.discard(ticket);

        assertTrue(globalUserManagementService.deleteUser(userReg.getCredentials().getUsername()));
    }


    @Test(groups = { "functional", "deployment" })
    public void validateUser() throws JsonProcessingException {
        // conflict with the email of a user outside the current tenant
        UserRegistration conflictingUserReg = createUserRegistration();
        createUser(
            conflictingUserReg.getCredentials().getUsername(),
            conflictingUserReg.getUser().getEmail(),
            conflictingUserReg.getUser().getFirstName(),
            conflictingUserReg.getUser().getLastName()
        );

        ObjectMapper mapper = new ObjectMapper();
        UserRegistration uReg = mapper.treeToValue(mapper.valueToTree(conflictingUserReg), UserRegistration.class);
        uReg.getUser().setFirstName("John");
        uReg.getUser().setLastName("Dodge");

        String json = restTemplate.postForObject(getRestAPIHostPort() + "/pls/users", uReg, String.class);
        ResponseDocument<RegistrationResult> response = ResponseDocument.generateFromJSON(json, RegistrationResult.class);

        assertFalse(response.getResult().isValid());
        User conflictingUser = response.getResult().getConflictingUser();
        assertEquals(conflictingUser.getFirstName(), conflictingUserReg.getUser().getFirstName());
        assertEquals(conflictingUser.getLastName(), conflictingUserReg.getUser().getLastName());
        assertEquals(conflictingUser.getEmail(), conflictingUserReg.getUser().getEmail());

        // conflict with the email of a user in the current tenant
        grantDefaultRights(testTenant.getId(), conflictingUserReg.getCredentials().getUsername());
        json = restTemplate.postForObject(getRestAPIHostPort() + "/pls/users", uReg, String.class);
        response = ResponseDocument.generateFromJSON(json, RegistrationResult.class);
        assertFalse(response.getResult().isValid());
        assertNull(response.getResult().getConflictingUser());

        // valid email
        uReg.getUser().setEmail("jdodge@dodge.com");
        uReg.getCredentials().setUsername("jdodge@dodge.com");

        assertTrue(globalUserManagementService.deleteUser(uReg.getCredentials().getUsername()));

        json = restTemplate.postForObject(getRestAPIHostPort() + "/pls/users", uReg, String.class);
        response = ResponseDocument.generateFromJSON(json, RegistrationResult.class);
        assertTrue(response.getResult().isValid());

        assertTrue(globalUserManagementService.deleteUser(uReg.getCredentials().getUsername()));
        assertTrue(globalUserManagementService.deleteUser(conflictingUserReg.getCredentials().getUsername()));
    }

    @Test(groups = { "functional", "deployment" })
    public void getAllUsers() {
        Tenant tenant = new Tenant();
        tenant.setName("Test Tenant");
        tenant.setId("Test_" + UUID.randomUUID().toString());
        try {
            globalTenantManagementService.registerTenant(tenant);
            grantAdminRights(tenant.getId(), testUser.getUsername());
            UserDocument doc = loginAndAttach(testUser.getUsername(), tenant);
            addAuthHeader.setAuthValue(doc.getTicket().getData());

            String json = restTemplate.getForObject(getRestAPIHostPort() + "/pls/users", String.class);
            ResponseDocument<ArrayNode> response =  ResponseDocument.generateFromJSON(json, ArrayNode.class);
            ArrayNode users = response.getResult();
            assertEquals(users.size(), 0);

            for (int i = 0; i < 10; i++) {
                String username = "tester" + String.valueOf(i) + "@test.com";
                assertTrue(globalUserManagementService.deleteUser(username));
                createUser(username, username, "Test", "Tester");
                grantDefaultRights(tenant.getId(), username);
            }

            json = restTemplate.getForObject(getRestAPIHostPort() + "/pls/users", String.class);
            response =  ResponseDocument.generateFromJSON(json, ArrayNode.class);
            users = response.getResult();
            assertEquals(users.size(), 10);

            for (JsonNode user: users) {
                assertEquals(user.get("FirstName").asText(), "Test");
                assertEquals(user.get("LastName").asText(), "Tester");
            }

            globalAuthenticationService.discard(doc.getTicket());
        } finally {
            for (int i = 0; i < 10; i++) {
                globalUserManagementService.deleteUser("tester" + String.valueOf(i) + "@test.com");
            }
            globalTenantManagementService.discardTenant(tenant);
        }
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = { "functional", "deployment" })
    public void changePassword() {
        UserRegistration userReg = createUserRegistration();
        assertTrue(globalUserManagementService.deleteUser(userReg.getCredentials().getUsername()));

        String json = restTemplate.postForObject(getRestAPIHostPort() + "/pls/users", userReg, String.class);
        ResponseDocument<RegistrationResult> response1 = ResponseDocument.generateFromJSON(json, RegistrationResult.class);
        assertTrue(response1.isSuccess());
        assertNotNull(response1.getResult().getPassword());

        User newUser = globalUserManagementService.getUserByEmail(userReg.getUser().getEmail());
        assertEquals(newUser.getFirstName(), userReg.getUser().getFirstName());
        assertEquals(newUser.getLastName(), userReg.getUser().getLastName());
        assertEquals(newUser.getPhoneNumber(), userReg.getUser().getPhoneNumber());
        assertEquals(newUser.getTitle(), userReg.getUser().getTitle());

        // try log in new user
        String password = response1.getResult().getPassword();
        Ticket ticket = globalAuthenticationService.authenticateUser(userReg.getUser().getUsername(), DigestUtils.sha256Hex(password));
        assertEquals(ticket.getTenants().size(), 1);
        globalAuthenticationService.discard(ticket);

        UserUpdateData data = new UserUpdateData();
        data.setOldPassword(DigestUtils.sha256Hex("wrong"));
        data.setNewPassword(DigestUtils.sha256Hex("newpass"));

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>(data.toString(), headers);

        UserDocument doc = loginAndAttach(userReg.getCredentials().getUsername(), password);
        addAuthHeader.setAuthValue(doc.getTicket().getData());
        String url = getRestAPIHostPort() + "/pls/users/" + userReg.getCredentials().getUsername() + "/creds";
        ResponseEntity<ResponseDocument> response = restTemplate.exchange(url, HttpMethod.PUT, requestEntity, ResponseDocument.class);
        assertFalse(response.getBody().isSuccess());

        data.setOldPassword(DigestUtils.sha256Hex(password));
        requestEntity = new HttpEntity<>(data.toString(), headers);
        response = restTemplate.exchange(url, HttpMethod.PUT, requestEntity, ResponseDocument.class);
        assertEquals(response.getStatusCode().value(), 200);
        assertTrue(response.getBody().isSuccess());

        globalAuthenticationService.discard(doc.getTicket());

        ticket = globalAuthenticationService.authenticateUser(userReg.getCredentials().getUsername(), DigestUtils.sha256Hex("newpass"));
        globalAuthenticationService.discard(ticket);

        assertTrue(globalUserManagementService.deleteUser(userReg.getCredentials().getUsername()));
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = { "functional", "deployment" })
    public void updateRights() {
        UserUpdateData data = new UserUpdateData();
        Map<String, EntityAccessRightsData> rightsDataMap = new HashMap<>();
        EntityAccessRightsData rightsData = new EntityAccessRightsData();
        rightsData.setMayView(true);
        rightsDataMap.put("PLS_Models", rightsData);
        rightsDataMap.put("PLS_Configuration", rightsData);
        rightsDataMap.put("PLS_Reporting", rightsData);
        rightsDataMap.put("PLS_Users", rightsData);
        data.setRights(rightsDataMap);

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>(data.toString(), headers);

        String url = getRestAPIHostPort() + "/pls/users/" + testCreds.getUsername();
        ResponseEntity<ResponseDocument> response2 = restTemplate.exchange(url, HttpMethod.PUT, requestEntity, ResponseDocument.class);
        assertTrue(response2.getBody().isSuccess());

        List<String> rights = globalUserManagementService.getRights(testCreds.getUsername(), testTenant.getId());
        assertTrue(rights.contains("View_PLS_Users"));

        addAuthHeader.setAuthValue(generalDoc.getTicket().getData());
        try {
            restTemplate.exchange(url, HttpMethod.PUT, requestEntity, ResponseDocument.class);
        } catch (HttpClientErrorException e) {
            assertEquals(e.getStatusCode().value(), 403);
        }
    }

    @Test(groups = { "functional", "deployment" })
    public void updateAccessLevel() {
        testUpdateAccessLevel(AccessLevel.SUPER_ADMIN);
        testUpdateAccessLevel(AccessLevel.INTERNAL_ADMIN);
        testUpdateAccessLevel(AccessLevel.INTERNAL_USER);
        testUpdateAccessLevel(AccessLevel.EXTERNAL_ADMIN);
        testUpdateAccessLevel(AccessLevel.EXTERNAL_USER);

        // Test updating without sufficient access level
        UserUpdateData data = new UserUpdateData();
        data.setAccessLevel(AccessLevel.EXTERNAL_USER.name());

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>(data.toString(), headers);

        String url = getRestAPIHostPort() + "/pls/users/" + testCreds.getUsername();
        addAuthHeader.setAuthValue(generalDoc.getTicket().getData());
        try {
            restTemplate.exchange(url, HttpMethod.PUT, requestEntity, ResponseDocument.class);
        } catch (HttpClientErrorException e) {
            assertEquals(e.getStatusCode().value(), 403);
        }
    }

    @Test(groups = { "functional", "deployment" })
    public void delete() {
        Tenant tenant = new Tenant();
        tenant.setName("Test Tenant");
        tenant.setId("Test_" + UUID.randomUUID().toString());
        try {
            globalTenantManagementService.registerTenant(tenant);
            grantAdminRights(tenant.getId(), testUser.getUsername());
            UserDocument doc = loginAndAttach(testUser.getUsername(), tenant);
            addAuthHeader.setAuthValue(doc.getTicket().getData());

            String json = restTemplate.getForObject(getRestAPIHostPort() + "/pls/users", String.class);
            ResponseDocument<ArrayNode> response =  ResponseDocument.generateFromJSON(json, ArrayNode.class);
            ArrayNode users = response.getResult();
            assertEquals(users.size(), 0);

            for (int i = 0; i < 10; i++) {
                String username = "tester" + String.valueOf(i) + "@test.com";
                assertTrue(globalUserManagementService.deleteUser(username));
                createUser(username, username, "Test", "Tester");
                grantDefaultRights(tenant.getId(), username);
            }

            json = restTemplate.getForObject(getRestAPIHostPort() + "/pls/users", String.class);
            response =  ResponseDocument.generateFromJSON(json, ArrayNode.class);
            users = response.getResult();
            assertEquals(users.size(), 10);

            List<String> usersToBeDeleted = Arrays.asList("tester0@test.com", "tester1@test.com", "tester2@test.com");

            ObjectMapper mapper = new ObjectMapper();
            String param = mapper.valueToTree(usersToBeDeleted).toString();

            HttpHeaders headers = new HttpHeaders();
            headers.add("Content-Type", "application/json");
            headers.add("Accept", "application/json");
            HttpEntity<JsonNode> requestEntity = new HttpEntity<>(null, headers);
            ResponseEntity<String> responseEntity = restTemplate.exchange(
                getRestAPIHostPort() + "/pls/users?usernames=" + param,
                HttpMethod.DELETE,
                requestEntity,
                String.class
            );

            ResponseDocument<DeleteUsersResult> response2 = ResponseDocument.generateFromJSON(responseEntity.getBody(), DeleteUsersResult.class);
            assertEquals(response2.getResult().getSuccessUsers().size(), 3);
            assertEquals(response2.getResult().getFailUsers().size(), 0);

            json = restTemplate.getForObject(getRestAPIHostPort() + "/pls/users", String.class);
            response =  ResponseDocument.generateFromJSON(json, ArrayNode.class);
            users = response.getResult();
            assertEquals(users.size(), 7);

            // delete admin user
            usersToBeDeleted = Arrays.asList("tester3@test.com", "tester4@test.com", testCreds.getUsername());
            param = mapper.valueToTree(usersToBeDeleted).toString();

            responseEntity = restTemplate.exchange(
                getRestAPIHostPort() + "/pls/users?usernames=" + param,
                HttpMethod.DELETE,
                requestEntity,
                String.class
            );

            response2 = ResponseDocument.generateFromJSON(responseEntity.getBody(), DeleteUsersResult.class);
            assertEquals(response2.getResult().getSuccessUsers().size(), 2);
            assertEquals(response2.getResult().getFailUsers().size(), 1);

            json = restTemplate.getForObject(getRestAPIHostPort() + "/pls/users", String.class);
            response =  ResponseDocument.generateFromJSON(json, ArrayNode.class);
            users = response.getResult();
            assertEquals(users.size(), 5);

            responseEntity = restTemplate.exchange(
                getRestAPIHostPort() + "/pls/users?usernames=malformedstring",
                HttpMethod.DELETE,
                requestEntity,
                String.class
            );
            response2 = ResponseDocument.generateFromJSON(responseEntity.getBody(), DeleteUsersResult.class);
            assertFalse(response2.isSuccess());

            globalAuthenticationService.discard(doc.getTicket());
        } finally {
            for (int i = 0; i < 10; i++) {
                globalUserManagementService.deleteUser("tester" + String.valueOf(i)  + "@test.com");
            }
            globalTenantManagementService.discardTenant(tenant);
        }
    }

    private UserRegistration createUserRegistration() {
        UserRegistration userReg = new UserRegistration();

        User user = new User();
        user.setEmail("test" + UUID.randomUUID().toString() + "@test.com");
        user.setFirstName("Test");
        user.setLastName("Tester");
        user.setPhoneNumber("650-555-5555");
        user.setTitle("CEO");

        Credentials creds = new Credentials();
        creds.setUsername(user.getEmail());
        creds.setPassword("WillBeModifiedImmediately");

        user.setUsername(creds.getUsername());

        userReg.setUser(user);
        userReg.setCredentials(creds);

        return userReg;
    }

    private void testUpdateAccessLevel(AccessLevel accessLevel) {
        UserUpdateData data = new UserUpdateData();
        data.setAccessLevel(accessLevel.name());
        Map<String, EntityAccessRightsData> rightsDataMap = new HashMap<>();
        EntityAccessRightsData rightsData = new EntityAccessRightsData();
        rightsData.setMayView(true);
        rightsDataMap.put("PLS_Models", rightsData);
        rightsDataMap.put("PLS_Configuration", rightsData);
        rightsDataMap.put("PLS_Reporting", rightsData);
        rightsDataMap.put("PLS_Users", rightsData);
        data.setRights(rightsDataMap);

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>(data.toString(), headers);

        String url = getRestAPIHostPort() + "/pls/users/" + testCreds.getUsername();
        ResponseEntity<ResponseDocument> response = restTemplate.exchange(url, HttpMethod.PUT, requestEntity, ResponseDocument.class);
        assertTrue(response.getBody().isSuccess());

        List<String> rights = globalUserManagementService.getRights(testCreds.getUsername(), testTenant.getId());
        List<GrantedRight> expectedRights = accessLevel.getGrantedRights();
        assertEquals(rights.size(), expectedRights.size());
        for (GrantedRight grantedRight: expectedRights) {
            assertTrue(rights.contains(grantedRight.getAuthority()));
        }
    }
}
