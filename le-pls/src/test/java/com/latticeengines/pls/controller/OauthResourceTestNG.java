package com.latticeengines.pls.controller;

import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.Test;
import static org.testng.Assert.assertTrue;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class OauthResourceTestNG extends PlsFunctionalTestNGBase {

    @Test(groups = { "functional" })
    public void generateApiToken() {
        String token = restTemplate.getForObject(getRestAPIHostPort()
                + "/pls/oauth/generateApiToken?tenantId=MyTestingTenant", String.class);
        assertTrue(StringUtils.isNotEmpty(token));
    }
}
