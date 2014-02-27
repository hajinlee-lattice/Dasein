package com.latticeengines.perf.exposed.test;

import static org.testng.Assert.assertTrue;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PerfFunctionalTestBaseUnitTestNG {
    
    private PerfFunctionalTestBase testBase;
    
    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        FileUtils.deleteDirectory(new File("/tmp/ledpjob"));
        testBase = new PerfFunctionalTestBase("localhost", "test", "test");
    }

    @Test(groups = "unit", dependsOnMethods = { "beforeClass" })
    public void afterClass() {
        testBase.afterClass();
        assertTrue(new File("/tmp/ledpjob/STOP").exists());
    }

    @Test(groups = "unit")
    public void beforeClass() {
        testBase.beforeClass();
        assertTrue(new File("/tmp/ledpjob/START").exists());
    }
}
