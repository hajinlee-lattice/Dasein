package com.latticeengines.baton;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AppUnitTestNG {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {

    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {

    }

    @Test(groups = "unit")
    public void testSerializeDocument() {
        Assert.assertTrue(true);
    }
}