package com.latticeengines.apps.lp.controller;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.apps.lp.testframework.LPDeploymentTestNGBase;
import com.latticeengines.domain.exposed.oauth.OauthClientType;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class OneTimePasswordResourceDeploymentTestNG extends LPDeploymentTestNGBase {

    @Inject
    private Oauth2RestApiProxy oauth2RestApiProxy;

    @Value("${common.test.oauth.url}")
    protected String authHostPort;

    @Test(groups = "deployment")
    public void testOTP() {
        String userId = TestFrameworkUtils.generateTenantName();
        deleteOAuthUserIfExists(userId);
        String otp = oauth2RestApiProxy.createAPIToken(userId);
        Assert.assertNotNull(otp);

        OAuth2RestTemplate oAuth2RestTemplate = OAuth2Utils.getOauthTemplate(authHostPort, userId, otp,
                OauthClientType.PLAYMAKER.getValue());
        OAuth2AccessToken accessToken = OAuth2Utils.getAccessToken(oAuth2RestTemplate);
        Assert.assertNotNull(accessToken);

        deleteOAuthUserIfExists(userId);
    }

}
