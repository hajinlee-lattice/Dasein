package com.latticeengines.matchapi.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.StatusDocument;
import com.latticeengines.matchapi.testframework.MatchapiDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.matchapi.MatchHealthProxy;

@Component
public class HealthResourceDeploymentTestNG extends MatchapiDeploymentTestNGBase {

    private static final Log log = LogFactory.getLog(HealthResourceDeploymentTestNG.class);

    @Autowired
    protected MatchHealthProxy matchHealthProxy;

    @Test(groups = "deployment")
    public void testDnbRateLimitStatus() {
        StatusDocument output = matchHealthProxy.dnbRateLimitStatus();
        log.info("Dnb RateLimitStatus : " + output.getStatus());
        Assert.assertNotNull(output);
    }

}
