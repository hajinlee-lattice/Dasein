package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.apache.commons.codec.digest.DigestUtils;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class UserResourceTestNG extends PlsFunctionalTestNGBase {
    
    @Test(groups = "functional")
    public void login() {
        Credentials creds = new Credentials();
        creds.setUsername("admin");
        creds.setPassword(DigestUtils.sha256Hex("admin"));

        Session session = restTemplate.postForObject("http://localhost:8080/pls/user/login", creds, Session.class,
                new Object[] {});
        assertEquals(session.getRights().size(), 4);
        assertNotNull(session.getTicket());
        assertEquals(session.getTicket().getTenants().size(), 2);
    }

    @Test(groups = "functional")
    public void loginBadPassword() {
        Credentials creds = new Credentials();
        creds.setUsername("admin");
        creds.setPassword("admin");
        
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());

        try {
            restTemplate.postForObject("http://localhost:8080/pls/user/login", creds, Session.class,
                    new Object[] {});
        } catch (Exception e) {
            String code = e.getMessage();
            assertEquals(code, "401");
        }
    }
    
}
