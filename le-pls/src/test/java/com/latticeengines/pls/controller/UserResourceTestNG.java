package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.globalauth.authentication.impl.GlobalAuthenticationServiceImpl;
import com.latticeengines.pls.security.GrantedRight;

public class UserResourceTestNG extends PlsFunctionalTestNGBase {
    private static Log log = LogFactory.getLog(UserResourceTestNG.class);

    @Autowired
    private GlobalAuthenticationServiceImpl globalAuthenticationService;

    @BeforeClass(groups = { "functional", "deployment" })
    public void setup() {
        Ticket ticket = globalAuthenticationService.authenticateUser("admin", DigestUtils.sha256Hex("admin"));
        //grantRight(GrantedRight.EDIT_PLS_USERS, ticket.getTenants().get(0).getId(), "admin");
    }

    @AfterClass(groups = { "functional", "deployment" })
    public void tearDown() {
        Ticket ticket = globalAuthenticationService.authenticateUser("admin", DigestUtils.sha256Hex("admin"));
        //revokeRight(GrantedRight.EDIT_PLS_USERS, ticket.getTenants().get(0).getId(), "admin");
    }


    @SuppressWarnings("rawtypes")
    @Test(groups = { "functional", "deployment" })
    public void registerUser() throws Exception {
        UserDocument doc = loginAndAttach("admin");
        Tenant tenant = doc.getTicket().getTenants().get(0);
        UserRegistration userReg = createUserRegistration(tenant);

        addAuthHeader.setAuthValue(doc.getTicket().getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addAuthHeader }));

        try {
            Boolean response = restTemplate.postForObject(getRestAPIHostPort() + "/pls/users/add", userReg, Boolean.class, new HashMap<>());
            assertTrue(response);
        } catch (Exception e) {
            log.info("User has already been created.");
        }

        setupDb(tenant.getId(), null);

        doc = loginAndAttach(userReg.getCredentials().getUsername());
        addAuthHeader.setAuthValue(doc.getTicket().getData());
        List summaries = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        assertEquals(summaries.size(), 1);
    }

    private UserRegistration createUserRegistration(Tenant tenant) {
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

        userReg.setUser(user);
        userReg.setCredentials(creds);
        userReg.setTenant(tenant);

        return userReg;
    }
}
