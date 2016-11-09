package com.latticeengines.datacloud.match.service.impl;

import static java.lang.Thread.sleep;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.dnb.DnBKeyType;
import com.latticeengines.datacloud.match.exposed.service.DnBAuthenticationService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;

public class DnBAuthenticationServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(DnBAuthenticationServiceImplTestNG.class);

    @Autowired
    private DnBAuthenticationService dnBAuthenticationService;

    @Test(groups = "functional", enabled = true)
    public void testAuthentication() {
        String token1 = dnBAuthenticationService.requestToken(DnBKeyType.REALTIME);
        String token2 = dnBAuthenticationService.requestToken(DnBKeyType.REALTIME);

        // token1 should be equal to token2 since token2 came from loading cache
        Assert.assertEquals(token1, token2);

        // Test cache expiration
        try {
            sleep(70000);
        } catch (InterruptedException e) {
        }
        String token3 = dnBAuthenticationService.requestToken(DnBKeyType.REALTIME);
        Assert.assertNotEquals(token1, token3);

        String token4 = dnBAuthenticationService.refreshAndGetToken(DnBKeyType.REALTIME);
        Assert.assertNotEquals(token3, token4);
    }
}
