package com.latticeengines.baton;

import org.junit.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AppUnitTestNG {
    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {

    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {

    }

    @Test(groups = "unit")
    public void test() {
        Assert.assertTrue(true);
    }
}