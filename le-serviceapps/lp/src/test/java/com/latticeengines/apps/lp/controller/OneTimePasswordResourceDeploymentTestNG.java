package com.latticeengines.apps.lp.controller;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.apps.lp.testframework.LPDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class OneTimePasswordResourceDeploymentTestNG extends LPDeploymentTestNGBase {

    @Inject
    private Oauth2RestApiProxy oauth2RestApiProxy;

    @Test(groups = "deployment")
    public void testOTP() {
        String userId = TestFrameworkUtils.generateTenantName();
        deleteOAuthUserIfExists(userId);
        String otp = oauth2RestApiProxy.createAPIToken(userId);
        Assert.assertNotNull(otp);
        deleteOAuthUserIfExists(userId);
    }

}
