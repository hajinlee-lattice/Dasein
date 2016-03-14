package com.latticeengines.scoringapi.controller;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.scoringapi.functionalframework.ScoringApiControllerTestNGBase;

public class HealthCheckResourceTestNG extends ScoringApiControllerTestNGBase {

    @Test(groups = "functional", enabled = true)
    public void getHealthCheck() {
        String url = apiHostPort + "/score/health";
        String result = oAuth2RestTemplate.getForObject(url, String.class);
        Assert.assertNotNull(result);
        Assert.assertEquals(result, HealthCheckResource.MESSAGE);
    }
}
