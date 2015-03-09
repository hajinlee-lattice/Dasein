package com.latticeengines.pls.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.latticeengines.domain.exposed.pls.DeleteUsersResult;
import com.latticeengines.domain.exposed.pls.RegistrationResult;
import com.latticeengines.domain.exposed.pls.ResponseDocument;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.pls.UserUpdateData;
import com.latticeengines.domain.exposed.security.EntityAccessRightsData;
import com.latticeengines.pls.globalauth.authentication.GlobalTenantManagementService;
import com.latticeengines.pls.globalauth.authentication.GlobalUserManagementService;
import com.latticeengines.pls.globalauth.authentication.impl.GlobalAuthenticationServiceImpl;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNotNull;


public class UserResourceTestNG extends PlsFunctionalTestNGBase {
    private static Log log = LogFactory.getLog(UserResourceTestNG.class);

    @Autowired
    private GlobalAuthenticationServiceImpl globalAuthenticationService;

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @Autowired
    private GlobalTenantManagementService globalTenantManagementService;

    private String testTenant;
    private UserRegistration testReg;
    private User testUser;
    private Credentials testCreds;
    private UserDocument adminDoc;

    @BeforeClass(groups = { "functional", "deployment" })
    public void setup() throws Exception {
        try {
            globalUserManagementService.deleteUser("hford");
            globalUserManagementService.deleteUser("hford@ford.com");
            globalUserManagementService.deleteUser(testCreds.getUsername());
        } catch (Exception e) {
            //ignore
        }

        testTenant =
            globalAuthenticationService.authenticateUser("admin", DigestUtils.sha256Hex("admin"))
                .getTenants().get(0).getId();

        testReg = createUserRegistration();
        testUser = testReg.getUser();
        testCreds = testReg.getCredentials();
        adminDoc = loginAndAttach("admin", "admin");
        addAuthHeader.setAuthValue(adminDoc.getTicket().getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addAuthHeader}));
        setupDb(testTenant, null);
    }

    @AfterClass(groups = { "functional", "deployment" })
    public void tearDown() {
        globalUserManagementService.deleteUser(testCreds.getUsername());
        globalAuthenticationService.discard(adminDoc.getTicket());
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = { "functional", "deployment" })
    public void registerUser() {
        globalUserManagementService.deleteUser(testCreds.getUsername());

        addAuthHeader.setAuthValue(adminDoc.getTicket().getData());
        String json = restTemplate.postForObject(getRestAPIHostPort() + "/pls/users", testReg, String.class);
        ResponseDocument<RegistrationResult> response = ResponseDocument.generateFromJSON(json, RegistrationResult.class);

        assertTrue(response.isSuccess());
        assertNotNull(response.getResult().getPassword());

        User newUser = globalUserManagementService.getUserByEmail(testUser.getEmail());
        assertEquals(newUser.getFirstName(), testUser.getFirstName());
        assertEquals(newUser.getLastName(), testUser.getLastName());
        assertEquals(newUser.getPhoneNumber(), testUser.getPhoneNumber());
        assertEquals(newUser.getTitle(), testUser.getTitle());

        String password = response.getResult().getPassword();
        Ticket ticket = globalAuthenticationService.authenticateUser(testCreds.getUsername(), DigestUtils.sha256Hex(password));
        assertEquals(ticket.getTenants().size(), 1);

        globalAuthenticationService.discard(ticket);
        globalUserManagementService.deleteUser(testCreds.getUsername());
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = { "functional", "deployment" })
    public void validateUser() throws JsonProcessingException {
        UserDocument doc;
        ObjectMapper mapper = new ObjectMapper();

        globalUserManagementService.deleteUser(testCreds.getUsername());
        createUser(testUser.getUsername(), testUser.getEmail(), testUser.getFirstName(), testUser.getLastName());
        grantDefaultRights(testTenant, testUser.getUsername());

        doc = loginAndAttach(testCreds.getUsername());
        addAuthHeader.setAuthValue(doc.getTicket().getData());

        // conflict with the email of a non-admin user
        UserRegistration uReg = mapper.treeToValue(mapper.valueToTree(testReg), UserRegistration.class);
        User newUser = mapper.treeToValue(mapper.valueToTree(testUser), User.class);
        newUser.setFirstName("John");
        newUser.setLastName("Dodge");
        uReg.setUser(newUser);
        addAuthHeader.setAuthValue(adminDoc.getTicket().getData());
        String json = restTemplate.postForObject(getRestAPIHostPort() + "/pls/users", uReg, String.class);
        ResponseDocument<RegistrationResult> response = ResponseDocument.generateFromJSON(json, RegistrationResult.class);

        assertFalse(response.getResult().isValid());
        User conflictingUser = response.getResult().getConflictingUser();
        assertEquals(conflictingUser.getFirstName(), testUser.getFirstName());
        assertEquals(conflictingUser.getLastName(), testUser.getLastName());
        assertEquals(conflictingUser.getEmail(), testUser.getEmail());

//        // conflict with the email of an admin user
//        grantAdminRights(doc.getTicket().getTenants().get(0).getId(), testUser.getUsername());
//
//        json = restTemplate.postForObject(getRestAPIHostPort() + "/pls/users", uReg, String.class);
//        response = ResponseDocument.generateFromJSON(json, RegistrationResult.class);
//
//        assertFalse(response.getResult().isValid());
//        assertNull(response.getResult().getConflictingUser());

        // valid email
        newUser.setEmail("jdodge@dodge.com");
        uReg.setUser(newUser);
        Credentials newCreds = mapper.treeToValue(mapper.valueToTree(testCreds), Credentials.class);
        newCreds.setUsername("jdodge");
        uReg.setCredentials(newCreds);
        newUser.setUsername(newCreds.getUsername());

        globalUserManagementService.deleteUser(newCreds.getUsername());

        json = restTemplate.postForObject(getRestAPIHostPort() + "/pls/users", uReg, String.class);
        response = ResponseDocument.generateFromJSON(json, RegistrationResult.class);
        assertTrue(response.getResult().isValid());

        globalAuthenticationService.discard(doc.getTicket());
        globalUserManagementService.deleteUser(uReg.getCredentials().getUsername());
        globalUserManagementService.deleteUser(testCreds.getUsername());
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = { "functional", "deployment" })
    public void getAllUsers() {
        Tenant tenant = new Tenant();
        tenant.setName("Test Tenant");
        tenant.setId("Test_" + UUID.randomUUID().toString());
        try {
            globalTenantManagementService.registerTenant(tenant);
            globalUserManagementService.deleteUser(testCreds.getUsername());
            createUser(testUser.getUsername(), testUser.getEmail(), testUser.getFirstName(), testUser.getLastName());
            grantAdminRights(tenant.getId(), testUser.getUsername());

            UserDocument doc = loginAndAttach(testCreds.getUsername(), tenant);
            addAuthHeader.setAuthValue(doc.getTicket().getData());

            String json = restTemplate.getForObject(getRestAPIHostPort() + "/pls/users", String.class);
            ResponseDocument<ArrayNode> response =  ResponseDocument.generateFromJSON(json, ArrayNode.class);
            ArrayNode users = response.getResult();
            assertEquals(users.size(), 0);

            for (int i = 0; i < 10; i++) {
                String username = "tester" + String.valueOf(i);
                globalUserManagementService.deleteUser(username);
                createUser(username, username + "@test.com", "Test", "Tester");
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
                String username = "tester" + String.valueOf(i);
                globalUserManagementService.deleteUser(username);
                globalUserManagementService.deleteUser(username + "@test.com");
            }
            globalTenantManagementService.discardTenant(tenant);
        }

        globalUserManagementService.deleteUser(testCreds.getUsername());
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = { "functional", "deployment" })
    public void changePassword() {
        UserDocument doc;

        globalUserManagementService.deleteUser(testCreds.getUsername());
        createUser(testUser.getUsername(), testUser.getEmail(), testUser.getFirstName(), testUser.getLastName());
        grantDefaultRights(testTenant, testUser.getUsername());

        doc = loginAndAttach(testCreds.getUsername());
        addAuthHeader.setAuthValue(doc.getTicket().getData());

        UserUpdateData data = new UserUpdateData();
        data.setOldPassword(DigestUtils.sha256Hex("wrong"));
        data.setNewPassword(DigestUtils.sha256Hex("newpass"));

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>(data.toString(), headers);

        String url = getRestAPIHostPort() + "/pls/users/" + testCreds.getUsername() + "/creds";
        ResponseEntity<ResponseDocument> response = restTemplate.exchange(url, HttpMethod.PUT, requestEntity, ResponseDocument.class);
        assertFalse(response.getBody().isSuccess());

        data.setOldPassword(DigestUtils.sha256Hex("admin"));
        requestEntity = new HttpEntity<>(data.toString(), headers);
        response = restTemplate.exchange(url, HttpMethod.PUT, requestEntity, ResponseDocument.class);
        assertEquals(response.getStatusCode().value(), 200);
        assertTrue(response.getBody().isSuccess());

        globalAuthenticationService.discard(doc.getTicket());

        Ticket ticket = globalAuthenticationService.authenticateUser(testCreds.getUsername(), DigestUtils.sha256Hex("newpass"));
        globalAuthenticationService.discard(ticket);

        globalUserManagementService.deleteUser(testCreds.getUsername());
    }

//    @SuppressWarnings("rawtypes")
//    @Test(groups = { "functional", "deployment" })
//    public void updateRights() {
//        globalUserManagementService.deleteUser(testCreds.getUsername());
//        createUser(testUser.getUsername(), testUser.getEmail(), testUser.getFirstName(), testUser.getLastName());
//        grantDefaultRights(adminDoc.getTicket().getTenants().get(0).getId(), testUser.getUsername());
//
//        UserUpdateData data = new UserUpdateData();
//        Map<String, EntityAccessRightsData> rightsDataMap = new HashMap<>();
//        EntityAccessRightsData rightsData = new EntityAccessRightsData();
//        rightsData.setMayView(true);
//        rightsDataMap.put("PLS_Models", rightsData);
//        rightsDataMap.put("PLS_Configuration", rightsData);
//        rightsDataMap.put("PLS_Reporting", rightsData);
//        rightsDataMap.put("PLS_Users", rightsData);
//        data.setRights(rightsDataMap);
//
//        HttpHeaders headers = new HttpHeaders();
//        headers.add("Content-Type", "application/json");
//        headers.add("Accept", "application/json");
//        HttpEntity<String> requestEntity = new HttpEntity<>(data.toString(), headers);
//
//        addAuthHeader.setAuthValue(adminDoc.getTicket().getData());
//        String url = getRestAPIHostPort() + "/pls/users/" + testCreds.getUsername();
//        ResponseEntity<ResponseDocument> response2 = restTemplate.exchange(url, HttpMethod.PUT, requestEntity, ResponseDocument.class);
//        assertTrue(response2.getBody().isSuccess());
//
//        UserDocument doc = loginAndAttach(testCreds.getUsername());
//        addAuthHeader.setAuthValue(doc.getTicket().getData());
//        response2 = restTemplate.exchange(url, HttpMethod.PUT, requestEntity, ResponseDocument.class);
//        assertEquals(response2.getStatusCode().value(), 403);
//        globalAuthenticationService.discard(doc.getTicket());
//
//        //TODO:song check rights after GlobalAuth' GetRights works properly
//
//        globalUserManagementService.deleteUser(testCreds.getUsername());
//    }

    @SuppressWarnings("rawtypes")
    @Test(groups = { "functional", "deployment" })
    public void delete() {
        Tenant tenant = new Tenant();
        tenant.setName("Test Tenant");
        tenant.setId("Test_" + UUID.randomUUID().toString());
        try {
            globalUserManagementService.deleteUser(testCreds.getUsername());
            createUser(testUser.getUsername(), testUser.getEmail(), testUser.getFirstName(), testUser.getLastName());

            globalTenantManagementService.registerTenant(tenant);
            grantAdminRights(tenant.getId(), testUser.getUsername());

            UserDocument doc = loginAndAttach(testCreds.getUsername(), tenant);
            addAuthHeader.setAuthValue(doc.getTicket().getData());

            String json = restTemplate.getForObject(getRestAPIHostPort() + "/pls/users", String.class);
            ResponseDocument<ArrayNode> response =  ResponseDocument.generateFromJSON(json, ArrayNode.class);
            ArrayNode users = response.getResult();
            assertEquals(users.size(), 0);

            for (int i = 0; i < 10; i++) {
                String username = "tester" + String.valueOf(i);
                globalUserManagementService.deleteUser(username);
                createUser(username, username + "@test.com", "Test", "Tester");
                grantDefaultRights(tenant.getId(), username);
            }

            json = restTemplate.getForObject(getRestAPIHostPort() + "/pls/users", String.class);
            response =  ResponseDocument.generateFromJSON(json, ArrayNode.class);
            users = response.getResult();
            assertEquals(users.size(), 10);

            List<String> usersToBeDeleted = Arrays.asList("tester0", "tester1", "tester2");

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
            usersToBeDeleted = Arrays.asList("tester3", "tester4", testCreds.getUsername());
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
                String username = "tester" + String.valueOf(i);
                globalUserManagementService.deleteUser(username);
                globalUserManagementService.deleteUser(username + "@test.com");
            }
            globalTenantManagementService.discardTenant(tenant);

            globalUserManagementService.deleteUser(testCreds.getUsername());
        }
    }

    private UserRegistration createUserRegistration() {
        UserRegistration userReg = new UserRegistration();

        User user = new User();
        user.setEmail("hford@ford.com");
        user.setFirstName("Henry");
        user.setLastName("Ford");
        user.setPhoneNumber("650-555-5555");
        user.setTitle("CEO");

        Credentials creds = new Credentials();
        creds.setUsername("hford");
        creds.setPassword("EETAlfvFzCdm6/t3Ro8g89vzZo6EDCbucJMTPhYgWiE=");

        user.setUsername(creds.getUsername());

        userReg.setUser(user);
        userReg.setCredentials(creds);

        return userReg;
    }
}
