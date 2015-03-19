package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNotNull;

import org.apache.commons.codec.digest.DigestUtils;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class LoginResourceTestNG extends PlsFunctionalTestNGBase {

    @Test(groups = { "functional", "deployment" })
    public void login() {
        Credentials creds = new Credentials();
        creds.setUsername("admin");
        creds.setPassword(DigestUtils.sha256Hex("admin"));

        LoginDocument loginDoc = restTemplate.postForObject(getRestAPIHostPort() + "/pls/login", creds, LoginDocument.class,
                new Object[] {});
        assertTrue(loginDoc.getResult().getTenants().size() >= 2);
        assertNotNull(loginDoc.getData());
    }

    @Test(groups = { "functional", "deployment" })
    public void loginBadPassword() {
        Credentials creds = new Credentials();
        creds.setUsername("admin");
        creds.setPassword("admin");

        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());

        try {
            restTemplate.postForObject(getRestAPIHostPort() + "/pls/login", creds, Session.class,
                    new Object[] {});
        } catch (Exception e) {
            String code = e.getMessage();
            assertEquals(code, "401");
        }
    }

}
