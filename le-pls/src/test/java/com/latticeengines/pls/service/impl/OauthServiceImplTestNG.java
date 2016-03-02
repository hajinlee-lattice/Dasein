package com.latticeengines.pls.service.impl;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;
import static org.testng.Assert.assertTrue;
import com.latticeengines.pls.service.OauthService;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class OauthServiceImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private OauthService oauthService;

    @Test(groups = "functional")
    public void generateAPIToken() {
        String apiToken = oauthService.generateAPIToken("MyTestingTenant");
        assertTrue(StringUtils.isNotEmpty(apiToken));
    }
}
