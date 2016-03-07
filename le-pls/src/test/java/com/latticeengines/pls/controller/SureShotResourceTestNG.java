package com.latticeengines.pls.controller;

import static org.testng.Assert.assertTrue;

import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class SureShotResourceTestNG extends PlsFunctionalTestNGBase {

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setUpMarketoEloquaTestEnvironment();
    }

    @Test(groups = { "functional" })
    public void getCredentialAuthenticationLink() {
        switchToSuperAdmin();
        String token = restTemplate.getForObject(getRestAPIHostPort() + "/pls/oauth/createaccesstoken?tenantId="
                + mainTestingTenant.getId(), String.class);
        assertTrue(StringUtils.isNotEmpty(token));

        String url = restTemplate.getForObject(getRestAPIHostPort() + "/pls/sureshot/credentials?tenantId="
                + mainTestingTenant.getId() + "&crmType=marketo", String.class);
        System.out.println(url);
        assertTrue(StringUtils.isNotEmpty(url));
    }

    @Test(groups = { "functional" })
    public void getScoringSettingsLink() {
        switchToSuperAdmin();
        String token = restTemplate.getForObject(getRestAPIHostPort() + "/pls/oauth/createaccesstoken?tenantId="
                + mainTestingTenant.getId(), String.class);
        assertTrue(StringUtils.isNotEmpty(token));

        String url = restTemplate.getForObject(getRestAPIHostPort() + "/pls/sureshot/scoring/settings/?tenantId="
                + mainTestingTenant.getId() + "&crmType=marketo", String.class);
        System.out.println(url);
        assertTrue(StringUtils.isNotEmpty(url));
    }
}
