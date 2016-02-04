package com.latticeengines.scoringapi.model;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.scoringapi.functionalframework.ScoringApiFunctionalTestNGBase;

public class ModelRetrieverTestNG extends ScoringApiFunctionalTestNGBase {

    @BeforeMethod(groups = "functional")
    public void beforeMethod() throws Exception {
    }

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
    }

    @AfterMethod(enabled = true, lastTimeOnly = true, alwaysRun = true)
    public void afterEachTest() {
    }

    @Test(groups = "functional")
    public void testSomething() throws Exception {

    }
}
