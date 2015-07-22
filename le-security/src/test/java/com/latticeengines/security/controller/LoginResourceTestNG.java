package com.latticeengines.security.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.apache.commons.codec.digest.DigestUtils;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;

public class LoginResourceTestNG extends SecurityFunctionalTestNGBase {

    @Test(groups = { "functional", "deployment" })
    public void login() {
        Credentials creds = new Credentials();
        creds.setUsername(adminUsername);
        creds.setPassword(DigestUtils.sha256Hex(adminPassword));

        LoginDocument loginDoc = restTemplate.postForObject(getRestAPIHostPort() + "/login", creds, LoginDocument.class);
        assertTrue(loginDoc.getResult().getTenants().size() >= 2);
        assertNotNull(loginDoc.getData());
    }

    @Test(groups = { "functional", "deployment" })
    public void loginBadPassword() {
        Credentials creds = new Credentials();
        creds.setUsername(adminUsername);
        creds.setPassword("badpassword");

        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());

        try {
            restTemplate.postForObject(getRestAPIHostPort() + "/login", creds, Session.class);
        } catch (Exception e) {
            String code = e.getMessage();
            assertEquals(code, "401");
        }
    }

}
