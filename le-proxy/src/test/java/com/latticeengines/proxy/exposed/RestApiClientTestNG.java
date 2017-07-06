package com.latticeengines.proxy.exposed;

import static org.testng.Assert.assertTrue;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.ResourceAccessException;
import org.testng.Assert;
import org.testng.annotations.Test;

@ContextConfiguration(locations = { "classpath:test-proxy-context.xml" })
public class RestApiClientTestNG extends AbstractTestNGSpringContextTests {

    private static final String PUBLIC_URL = "https://app.lattice-engines.com";
    private static final String PRIVATE_URL = "https://lpi-app-1206577515.us-east-1.elb.amazonaws.com";

    @Autowired
    private ApplicationContext applicationContext;

    @Test(groups = "functional")
    public void testInstantiateAndGet() {
        RestApiClient client = RestApiClient.newExternalClient(applicationContext, PUBLIC_URL);
        String page = client.get("/");
        Assert.assertTrue(StringUtils.isNotEmpty(page), String.format("Website %s gives empty page", PUBLIC_URL));

        client = RestApiClient.newInternalClient(applicationContext, PRIVATE_URL);
        page = client.get("/");
        Assert.assertTrue(StringUtils.isNotEmpty(page), String.format("Website %s gives empty page", PRIVATE_URL));
    }

    @Test(groups = "functional")
    public void testFailSSL() {
        boolean thrown = false;
        try {
            RestApiClient client = RestApiClient.newExternalClient(applicationContext, PRIVATE_URL);
            client.setMaxAttempts(3);
            client.get("/");
        } catch (Exception e) {
            assertTrue(e instanceof ResourceAccessException);
            thrown = true;
        }
        assertTrue(thrown);
    }
}
