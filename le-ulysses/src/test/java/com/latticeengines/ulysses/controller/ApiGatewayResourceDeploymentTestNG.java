package com.latticeengines.ulysses.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ulysses.GatewayPolicyConfiguration;
import com.latticeengines.ulysses.testframework.UlyssesDeploymentTestNGBase;

public class ApiGatewayResourceDeploymentTestNG extends UlyssesDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ApiGatewayResourceDeploymentTestNG.class);

    private String getAttributeResourceUrl() {
        return ulyssesHostPort + "/ulysses/api-gateway";
    }
    
    @Test(groups = "deployment")
    public void testGetGatewayPolicyConfiguration() {
        String url = getAttributeResourceUrl() + "/tenant-config";
        GatewayPolicyConfiguration gatewayPolicyConfig = getOAuth2RestTemplate().getForObject(url, GatewayPolicyConfiguration.class);
        
        Assert.assertNotNull(gatewayPolicyConfig);
        Assert.assertNotNull(gatewayPolicyConfig.getApiKey());
        Assert.assertEquals(gatewayPolicyConfig.getApiKey(), super.GW_API_KEY);
    }
    
}
