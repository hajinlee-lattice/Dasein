package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.domain.exposed.pls.Oauth2AccessToken;
import com.latticeengines.network.exposed.oauth.Oauth2Interface;
import com.latticeengines.pls.entitymanager.Oauth2AccessTokenEntityMgr;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class Oauth2ServiceImplDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Autowired
    private Oauth2Interface oauth2Service;

    @Autowired
    private Oauth2AccessTokenEntityMgr oauth2AccessTokenEntityMgr;

    private String tenantId;

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
        assertTrue(hasTokenValue(oauth2AccessTokenEntityMgr.findAll(), accessToken), "Cannot find the new token in DB.");
        assertEquals(oauth2AccessTokenEntityMgr.get(tenantId).getAccessToken(), accessToken.getValue());

        OAuth2AccessToken accessToken2 = oauth2Service.createOAuth2AccessToken(tenantId);
        assertTrue(StringUtils.isNotEmpty(accessToken2.getValue()));
        assertFalse(hasTokenValue(oauth2AccessTokenEntityMgr.findAll(), accessToken), "Should not have the old token in DB.");
        assertTrue(hasTokenValue(oauth2AccessTokenEntityMgr.findAll(), accessToken2), "Cannot find the new token in DB.");
        assertEquals(oauth2AccessTokenEntityMgr.get(tenantId).getAccessToken(), accessToken2.getValue());
        assertNotEquals(oauth2AccessTokenEntityMgr.get(tenantId).getAccessToken(), accessToken.getValue());
    }

    private boolean hasTokenValue(List<Oauth2AccessToken> tokens, OAuth2AccessToken targetToken) {
        String value =targetToken.getValue();
        for (Oauth2AccessToken token: tokens) {
            if (value.equals(CipherUtils.decrypt(token.getAccessToken()))) {
                return true;
            }
        }
        return false;
    }

}
