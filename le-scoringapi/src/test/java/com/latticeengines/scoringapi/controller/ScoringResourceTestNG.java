package com.latticeengines.scoringapi.controller;

import org.testng.annotations.Test;

import com.latticeengines.scoringapi.functionalframework.ScoringApiFunctionalTestNGBase;

public class ScoringResourceTestNG extends ScoringApiFunctionalTestNGBase {

    @Test(groups = "functional")
    public void getModels() {
        String url = apiHostPort + "/score/models/CONTACT";
        String result = oAuth2RestTemplate.getForObject(url, String.class);
//        Assert.assertNotNull(result);

    }
}
