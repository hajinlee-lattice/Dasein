package com.latticeengines.scoringapi.controller;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.StatusDocument;
import com.latticeengines.scoringapi.functionalframework.ScoringApiControllerDeploymentTestNGBase;

public class HealthCheckResourceDeploymentTestNG extends ScoringApiControllerDeploymentTestNGBase {

    @Test(groups = "deployment", enabled = true)
    public void getHealthCheck() {
        String url = apiHostPort + "/score/health";
        StatusDocument result = oAuth2RestTemplate.getForObject(url, StatusDocument.class);
        Assert.assertNotNull(result);
        Assert.assertEquals(JsonUtils.serialize(result), JsonUtils.serialize(StatusDocument.online()));
    }
}
