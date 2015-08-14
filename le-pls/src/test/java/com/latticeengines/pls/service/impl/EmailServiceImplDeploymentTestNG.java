package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.net.URI;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.pls.RegistrationResult;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.security.exposed.AccessLevel;

import microsoft.exchange.webservices.data.core.ExchangeService;
import microsoft.exchange.webservices.data.core.enumeration.misc.ExchangeVersion;
import microsoft.exchange.webservices.data.credential.ExchangeCredentials;
import microsoft.exchange.webservices.data.credential.WebCredentials;
import microsoft.exchange.webservices.data.search.ItemView;

public class EmailServiceImplDeploymentTestNG extends PlsFunctionalTestNGBase {

    private static final String INTERNAL_USER = "build@lattice-engines.com";
    private static final String EXTERNAL_USER = "build.lattice.engines@gmail.com";

    @BeforeClass(groups = "deployment")
    public void setup() {
        switchToSuperAdmin();
    }

    @AfterClass(groups = { "deployment" })
    public void tearDown() { }

    @Test(groups = "deployment")
    public void testSendAndReceiveInternalEmail() {
        createNewUserAndSendEmail(INTERNAL_USER);    }

    @Test(groups = "deployment")
    public void testSendAndReceiveExternalEmail() {
        createNewUserAndSendEmail(EXTERNAL_USER);
    }

    private void createNewUserAndSendEmail(String email) {
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

        deleteUserWithUsername(userReg.getCredentials().getUsername());

        String json = restTemplate.postForObject(getRestAPIHostPort() + "/pls/users/", userReg, String.class);
        ResponseDocument<RegistrationResult> response = ResponseDocument.generateFromJSON(json,
                RegistrationResult.class);
        assertNotNull(response);
        assertTrue(response.isSuccess());
        assertNotNull(response.getResult().getPassword());
    }

    @SuppressWarnings("unused")
    private void verifyReceivedEmailInOutlook() {
        try {
            ExchangeService service = new ExchangeService(ExchangeVersion.Exchange2010_SP2);
            ExchangeCredentials credentials = new WebCredentials("build@lattice-engines.com", "");
            service.setCredentials(credentials);
            service.setUrl(new URI("https://mail.lattice-engines.com/ews/exchange.asmx"));
            ItemView view = new ItemView(50);
            // FindItemsResults<Item> findResults = service.findItems(WellKnownFolderName.Inbox, view);
        } catch (Exception e) {
            Assert.fail("Failed to verify email.", e);
        }
    }

    @SuppressWarnings("unused")
    private void verifyReceivedEmailInGmail() {

    }

}
