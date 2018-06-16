package com.latticeengines.matchapi.controller;

import javax.inject.Inject;

import org.testng.annotations.Test;

import com.latticeengines.matchapi.testframework.MatchapiFunctionalTestNGBase;

public class HealthResourceTestNG extends MatchapiFunctionalTestNGBase {

    @Inject
    private HealthResource healthResource;

    @Test(groups = "functional")
    public void testMatch() {
        healthResource.healthCheck();
    }


}
