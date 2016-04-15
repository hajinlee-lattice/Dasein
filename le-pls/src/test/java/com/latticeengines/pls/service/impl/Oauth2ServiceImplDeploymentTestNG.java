package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.network.exposed.oauth.Oauth2Interface;
import com.latticeengines.pls.entitymanager.Oauth2AccessTokenEntityMgr;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class Oauth2ServiceImplDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Autowired
    private Oauth2Interface oauth2Service;

    @Autowired
    private Oauth2AccessTokenEntityMgr oauth2AccessTokenEntityMgr;

    private String tenantId = "Oauth2Tenant";

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        tenantId = mainTestTenant.getId();
    }

    @Test(groups = "deployment")
    public void createAPIToken() {
        String apiToken = oauth2Service.createAPIToken("MyTestingTenant");
        assertTrue(StringUtils.isNotEmpty(apiToken));
    }

    @Test(groups = "deployment")
    public void createAccessAndRefreshToken() {
        setupSecurityContext(mainTestTenant);
        assertEquals(oauth2AccessTokenEntityMgr.get(tenantId).getAccessToken(), "");

        OAuth2AccessToken accessToken = oauth2Service.createOAuth2AccessToken(tenantId);
        assertTrue(StringUtils.isNotEmpty(accessToken.getValue()));
        assertEquals(oauth2AccessTokenEntityMgr.findAll().size(), 1);
        assertEquals(oauth2AccessTokenEntityMgr.get(tenantId).getAccessToken(), accessToken.getValue());

        OAuth2AccessToken accessToken2 = oauth2Service.createOAuth2AccessToken(tenantId);
        assertTrue(StringUtils.isNotEmpty(accessToken2.getValue()));
        assertEquals(oauth2AccessTokenEntityMgr.findAll().size(), 1);
        assertEquals(oauth2AccessTokenEntityMgr.get(tenantId).getAccessToken(), accessToken2.getValue());
        assertNotEquals(oauth2AccessTokenEntityMgr.get(tenantId).getAccessToken(), accessToken.getValue());
    }
}
