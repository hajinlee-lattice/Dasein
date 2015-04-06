package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.security.AccessLevel;

public class LoginResourceTestNG extends PlsFunctionalTestNGBase {

    @Test(groups = { "functional", "deployment" })
    public void login() {
        Credentials creds = new Credentials();
        creds.setUsername(adminUsername);
        creds.setPassword(DigestUtils.sha256Hex(adminPassword));

        LoginDocument loginDoc = restTemplate.postForObject(getRestAPIHostPort() + "/pls/login", creds, LoginDocument.class);
        assertTrue(loginDoc.getResult().getTenants().size() >= 2);
        assertNotNull(loginDoc.getData());
    }

    @Test(groups = { "functional", "deployment" })
    public void loginAndAttach() {
        Credentials creds = new Credentials();
        creds.setUsername(adminUsername);
        creds.setPassword(DigestUtils.sha256Hex(adminPassword));

        LoginDocument loginDoc = restTemplate.postForObject(getRestAPIHostPort() + "/pls/login", creds, LoginDocument.class);
        assertTrue(loginDoc.getResult().getTenants().size() >= 2);
        assertNotNull(loginDoc.getData());

        addAuthHeader.setAuthValue(loginDoc.getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addAuthHeader}));

        UserDocument uDoc = restTemplate.postForObject(
                getRestAPIHostPort() + "/pls/attach", loginDoc.getResult().getTenants().get(0), UserDocument.class);
        assertEquals(uDoc.getResult().getUser().getAccessLevel(), AccessLevel.SUPER_ADMIN.name());
    }

    @Test(groups = { "functional", "deployment" })
    public void loginBadPassword() {
        Credentials creds = new Credentials();
        creds.setUsername(adminUsername);
        creds.setPassword("badpassword");

        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());

        try {
            restTemplate.postForObject(getRestAPIHostPort() + "/pls/login", creds, Session.class);
        } catch (Exception e) {
            String code = e.getMessage();
            assertEquals(code, "401");
        }
    }

}
