package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.pls.RegistrationResult;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.security.exposed.AccessLevel;

public class EmailServiceImplDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String INTERNAL_USER_EMAIL = "build@lattice-engines.com";
    private static final String EXTERNAL_USER_EMAIL = "build.lattice.engines@gmail.com";

    private String testUsername;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironment();
        deleteUserByRestCall(INTERNAL_USER_EMAIL);
        deleteUserByRestCall(EXTERNAL_USER_EMAIL);
    }

    @AfterClass(groups = { "deployment" })
    public void tearDown() {
        deleteUserByRestCall(INTERNAL_USER_EMAIL);
        deleteUserByRestCall(EXTERNAL_USER_EMAIL);
    }

    @Test(groups = "deployment")
    public void testSendAndReceiveInternalEmail() {
        createNewUserAndSendEmail(INTERNAL_USER_EMAIL);
    }

    @Test(groups = "deployment")
    public void testSendAndReceiveExternalEmail() {
        createNewUserAndSendEmail(EXTERNAL_USER_EMAIL);
    }

    private void createNewUserAndSendEmail(String email) {
        UserRegistration userReg = getUserReg(email);
        deleteUserByRestCall(testUsername);

        String json = restTemplate.postForObject(getRestAPIHostPort() + "/pls/users/", userReg, String.class);
        ResponseDocument<RegistrationResult> response = ResponseDocument.generateFromJSON(json,
                RegistrationResult.class);
        assertNotNull(response);
        assertTrue(response.isSuccess(), JsonUtils.serialize(response));
        assertNotNull(response.getResult().getPassword());

        deleteUserByRestCall(testUsername);
    }

    private UserRegistration getUserReg(String email) {
        UserRegistration userReg = new UserRegistration();

        User user = new User();
        user.setEmail(email);
        user.setFirstName("Lattice");
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

        testUsername = userReg.getCredentials().getUsername();

        return userReg;
    }

    @SuppressWarnings("unused")
    private void verifyReceivedEmailInOutlook() {
    }

    @SuppressWarnings("unused")
    private void verifyReceivedEmailInGmail() {

    }

}
