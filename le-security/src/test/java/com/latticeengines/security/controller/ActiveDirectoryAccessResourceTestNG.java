package com.latticeengines.security.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;

public class ActiveDirectoryAccessResourceTestNG extends SecurityFunctionalTestNGBase {
    
    private String token;

    @SuppressWarnings("unchecked")
    @Test(groups = "functional", dependsOnMethods = "loginWithActiveDirectoryAuthentication", enabled = true)
    public void getSomethingWithAccess() {
        addAuthHeader.setAuthValue(token);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addAuthHeader }));
        Map<String, String> retVal = restTemplate.getForObject(getRestAPIHostPort() + "/adhasaccess", Map.class, new HashMap<>());
        assertTrue(retVal.containsKey("SomeReturnValue"));
    }

    @Test(groups = "functional", dependsOnMethods = "loginWithActiveDirectoryAuthentication", enabled = true)
    public void getSomethingWithoutAccess() {
        addAuthHeader.setAuthValue(token);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addAuthHeader }));
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());
        boolean exception = false;
        try {
            restTemplate.getForObject(getRestAPIHostPort() + "/adnoaccess", Map.class, new HashMap<>());
        } catch (Exception e) {
            exception = true;
            String code = e.getMessage();
            assertEquals(code, "403");
        }
        assertTrue(exception);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "functional", enabled = true)
    public void loginWithActiveDirectoryAuthentication() {
        Credentials creds = new Credentials();
        creds.setUsername("testuser1");
        creds.setPassword("Lattice1");
        
        Map<String, String> map = restTemplate.postForObject(getRestAPIHostPort() + "/adlogin", creds, Map.class);
        token = map.get("Token");
        assertNotNull(token);
    }
}
