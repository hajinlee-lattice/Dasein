package com.latticeengines.pls.service.impl;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.network.exposed.oauth.Oauth2Interface;
import com.latticeengines.pls.entitymanager.Oauth2AccessTokenEntityMgr;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBaseDeprecated;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.service.TenantService;

public class Oauth2ServiceImplDeploymentTestNG extends PlsDeploymentTestNGBaseDeprecated {

    @Autowired
    private Oauth2Interface oauth2Service;

    @Autowired
    private Oauth2AccessTokenEntityMgr oauth2AccessTokenEntityMgr;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    private String tenantId = "Oauth2Tenant";

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        Tenant t = tenantService.findByTenantId(tenantId);
        if (t != null) {
            tenantService.discardTenant(t);
        }
    }

    @AfterClass(groups = "deployment")
    public void teardown() throws Exception {
        Tenant t = tenantService.findByTenantId(tenantId);
        if (t != null) {
            tenantService.discardTenant(t);
        }
    }

    @Test(groups = "deployment")
    public void createAPIToken() {
        String apiToken = oauth2Service.createAPIToken("MyTestingTenant");
        assertTrue(StringUtils.isNotEmpty(apiToken));
    }

    @Test(groups = "deployment")
    public void createAccessAndRefreshToken() {
        Tenant t = new Tenant();
        t.setId(tenantId);
        t.setName(tenantId);
        tenantEntityMgr.create(t);
        setupSecurityContext(t);
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
