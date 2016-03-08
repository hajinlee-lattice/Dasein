package com.latticeengines.pls.controller;

import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class Oauth2ResourceTestNG extends PlsFunctionalTestNGBase {

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setUpMarketoEloquaTestEnvironment();
    }

    @Test(groups = { "functional" })
    public void createApiToken() {
        switchToExternalAdmin();
        String token = restTemplate.getForObject(getRestAPIHostPort() + "/pls/oauth2/apitoken?tenantId="
                + mainTestingTenant.getId(), String.class);
        assertTrue(StringUtils.isNotEmpty(token));
    }

    @Test(groups = { "functional" })
    public void createOAuth2AccessToken() {
        switchToThirdPartyUser();
        String token = restTemplate.getForObject(getRestAPIHostPort()
                + "/pls/oauth2/accesstoken?tenantId=" + mainTestingTenant.getId(), String.class);
        assertTrue(StringUtils.isNotEmpty(token));
    }
}
