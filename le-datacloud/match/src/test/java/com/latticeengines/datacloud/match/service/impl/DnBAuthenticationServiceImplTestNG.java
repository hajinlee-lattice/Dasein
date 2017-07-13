package com.latticeengines.datacloud.match.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.service.DnBAuthenticationService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;

public class DnBAuthenticationServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DnBAuthenticationServiceImplTestNG.class);

    @Autowired
    private DnBAuthenticationService dnBAuthenticationService;

    @Test(groups = "dnb", enabled = true)
    public void testAuthentication() {
        String token1 = dnBAuthenticationService.requestToken(DnBKeyType.REALTIME);
        String token2 = dnBAuthenticationService.requestToken(DnBKeyType.REALTIME);
        Assert.assertNotNull(token1);
        Assert.assertNotNull(token2);

        // token1 should be equal to token2 since token2 came from loading cache
        Assert.assertEquals(token1, token2);

        log.info("Token1: " + token1);

        dnBAuthenticationService.refreshToken(DnBKeyType.REALTIME);

        String token3 = dnBAuthenticationService.requestToken(DnBKeyType.REALTIME);
        Assert.assertNotNull(token3);
        Assert.assertNotEquals(token1, token3);

        log.info("Token3: " + token3);
    }
}
