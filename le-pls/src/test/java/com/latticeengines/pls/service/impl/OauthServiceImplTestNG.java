package com.latticeengines.pls.service.impl;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

import com.latticeengines.pls.service.OauthService;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class OauthServiceImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private OauthService oauthService;

    @Test(groups = "functional")
    public void createAPIToken() {
        String apiToken = oauthService.createAPIToken("MyTestingTenant");
        assertTrue(StringUtils.isNotEmpty(apiToken));
    }

    @Test(groups = "functional")
    public void createAccessAndRefreshToken() {
        OAuth2AccessToken accessToken = oauthService.createOAuth2AccessToken("MyTestingTenant");
        assertTrue(StringUtils.isNotEmpty(accessToken.getValue()));
    }
}
