package com.latticeengines.baton;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AppUnitTestNG {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    @BeforeMethod(groups = "unit")
    public void setUp() {

    }

    @AfterMethod(groups = "unit")
    public void tearDown() {

    }

    @Test(groups = "unit")
    public void test() {
        log.info("Test started at {}", new java.util.Date());
        Assert.assertTrue(true);
    }
}