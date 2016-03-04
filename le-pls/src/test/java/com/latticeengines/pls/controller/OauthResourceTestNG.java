package com.latticeengines.pls.controller;

import org.apache.commons.lang3.StringUtils;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class OauthResourceTestNG extends PlsFunctionalTestNGBase {

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setUpMarketoEloquaTestEnvironment();
    }

    @Test(groups = { "functional" })
    public void createApiToken() {
        switchToThirdPartyUser();
        String token = restTemplate.getForObject(getRestAPIHostPort() + "/pls/oauth/createapitoken?tenantId="
                + mainTestingTenant.getId(), String.class);
        assertTrue(StringUtils.isNotEmpty(token));
    }

    @Test(groups = { "functional" })
    public void createAccessToken() {
        switchToThirdPartyUser();
        OAuth2AccessToken token = restTemplate.getForObject(getRestAPIHostPort()
                + "/pls/oauth/createacesstoken?tenantId=" + mainTestingTenant.getId(), OAuth2AccessToken.class);
        assertTrue(StringUtils.isNotEmpty(token.getValue()));
    }
}
