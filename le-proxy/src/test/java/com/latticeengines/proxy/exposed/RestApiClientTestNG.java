package com.latticeengines.proxy.exposed;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.ResourceAccessException;
import org.testng.Assert;
import org.testng.annotations.Test;

@ContextConfiguration(locations = { "classpath:test-proxy-context.xml" })
public class RestApiClientTestNG extends AbstractTestNGSpringContextTests {

    /*-
     * use stack a endpoint for now as functional test might run before stack b deployment
     * FIXME add deployment test for proxy project
     */
    private static final String PUBLIC_URL = "https://testapp.lattice-engines.com";
    private static final String PRIVATE_URL = "https://internal-private-lpi-a-1832171025.us-east-1.elb.amazonaws.com";

    @Inject
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

    @Test(groups = "functional", expectedExceptions = { ResourceAccessException.class })
    public void testFailSSL() {
        RestApiClient client = RestApiClient.newExternalClient(applicationContext, PRIVATE_URL);
        client.setMaxAttempts(3);
        client.get("/");
    }
}
