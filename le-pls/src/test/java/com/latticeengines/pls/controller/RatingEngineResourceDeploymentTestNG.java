package com.latticeengines.pls.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;

import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class RatingEngineResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineResourceDeploymentTestNG.class);

    private static final String RATING_ENGINE_NAME = "Rating Engine";
    private static final String RATING_ENGINE_NOTE = "This is a Rating Engine that covers North America market";
    private static final String SEGMENT_NAME = "segment";
    private static final String CREATED_BY = "lattice@lattice-engines.com";

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        mainTestTenant = testBed.getMainTestTenant();
        switchToSuperAdmin();
    }

    // TODO

}
