package com.latticeengines.modelquality.functionalframework;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public class ModelQualityFunctionalTestNGBase extends ModelQualityTestNGBase {

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
    }

    @AfterClass(groups = "functional")
    public void tearDown() throws Exception {
    }
}
