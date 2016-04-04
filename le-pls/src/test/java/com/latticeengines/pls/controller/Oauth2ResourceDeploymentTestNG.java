package com.latticeengines.pls.controller;

import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static org.testng.Assert.assertTrue;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;


public class Oauth2ResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironment();
    }

    @Test(groups = { "deployment" })
    public void createApiToken() {
        switchToExternalAdmin();
        String token = restTemplate.getForObject(getRestAPIHostPort() + "/pls/oauth2/apitoken?tenantId="
                + mainTestingTenant.getId(), String.class);
        assertTrue(StringUtils.isNotEmpty(token));
    }

    @Test(groups = { "deployment" })
    public void createOAuth2AccessToken() {
        switchToExternalAdmin();
        String token = restTemplate.getForObject(getRestAPIHostPort()
                + "/pls/oauth2/accesstoken?tenantId=" + mainTestingTenant.getId(), String.class);
        assertTrue(StringUtils.isNotEmpty(token));
    }
}
