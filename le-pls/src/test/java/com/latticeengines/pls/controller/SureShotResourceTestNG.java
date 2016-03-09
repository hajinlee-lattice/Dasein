package com.latticeengines.pls.controller;

import static org.testng.Assert.assertTrue;

import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.pls.SureShotUrls;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class SureShotResourceTestNG extends PlsFunctionalTestNGBase {

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setUpMarketoEloquaTestEnvironment();
    }

    @Test(groups = { "functional" })
    public void getCredentialAuthenticationLink() {
        switchToExternalAdmin();
        String token = restTemplate.getForObject(getRestAPIHostPort() + "/pls/oauth2/accesstoken?tenantId="
                + mainTestingTenant.getId(), String.class);
        assertTrue(StringUtils.isNotEmpty(token));

        String url = restTemplate.getForObject(getRestAPIHostPort() + "/pls/sureshot/credentials?crmType=marketo",
                String.class);
        System.out.println(url);
        assertTrue(StringUtils.isNotEmpty(url));
    }

    @SuppressWarnings("unchecked")
    @Test(groups = { "functional" })
    public void getSureShotUrls() {
        switchToSuperAdmin();
        String token = restTemplate.getForObject(getRestAPIHostPort() + "/pls/oauth2/accesstoken?tenantId="
                + mainTestingTenant.getId(), String.class);
        assertTrue(StringUtils.isNotEmpty(token));

        ResponseDocument<SureShotUrls> response = restTemplate.getForObject(getRestAPIHostPort()
                + "/pls/sureshot/urls?crmType=marketo", ResponseDocument.class);
        System.out.println(response);
        assertTrue(response.isSuccess());
        assertTrue(StringUtils.isNotEmpty(response.toString()));
    }
}
