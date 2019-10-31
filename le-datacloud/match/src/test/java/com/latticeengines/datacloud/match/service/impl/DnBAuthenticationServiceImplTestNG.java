package com.latticeengines.datacloud.match.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.service.DnBAuthenticationService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;

public class DnBAuthenticationServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DnBAuthenticationServiceImplTestNG.class);

    @Inject
    private DnBAuthenticationService dnBAuthenticationService;

    @Test(groups = "dnb", enabled = true)
    public void testAuthentication() {
        String token1 = dnBAuthenticationService.requestToken(DnBKeyType.REALTIME, null);
        String token2 = dnBAuthenticationService.requestToken(DnBKeyType.REALTIME, null);
        Assert.assertNotNull(token1);
        Assert.assertNotNull(token2);

        Assert.assertEquals(token1, token2);
    }
}
