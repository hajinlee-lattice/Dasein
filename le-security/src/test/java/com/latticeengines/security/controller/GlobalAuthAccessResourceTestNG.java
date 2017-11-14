package com.latticeengines.security.controller;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.functionalframework.UserResourceTestNGBase;

public class GlobalAuthAccessResourceTestNG extends UserResourceTestNGBase {

    @BeforeClass(groups = {"functional", "deployment"})
    public void setup() throws Exception {
        createTestTenant();
        createTestUsers();
    }

    @AfterClass(groups = {"functional", "deployment"})
    public void tearDown() {
        destroyTestUsers();
        destroyTestTenant();
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "functional")
    public void getPrincipal() {
        switchToAccessLevel(AccessLevel.SUPER_ADMIN);
        String principal = restTemplate.getForObject(getRestAPIHostPort() + "/garesource/principal", String.class);
        Assert.assertNotNull(principal);
        Assert.assertTrue(principal.startsWith("tester"));
        Assert.assertTrue(principal.endsWith("@test.lattice.com"));
    }
}
