package com.latticeengines.apps.lp.service.impl;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.apps.lp.service.OneTimePasswordService;
import com.latticeengines.apps.lp.testframework.LPFunctionalTestNGBase;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class OneTimePasswordServiceImplTestNG extends LPFunctionalTestNGBase {

    @Inject
    private OneTimePasswordService oneTimePasswordService;

    @Test(groups = "functional")
    public void testGenerateOPT() {
        String userId = TestFrameworkUtils.generateTenantName();
        deleteOAuthUserIfExists(userId);
        String password = oneTimePasswordService.generateOTP(userId);
        Assert.assertNotNull(password);
        deleteOAuthUserIfExists(userId);
    }

}
