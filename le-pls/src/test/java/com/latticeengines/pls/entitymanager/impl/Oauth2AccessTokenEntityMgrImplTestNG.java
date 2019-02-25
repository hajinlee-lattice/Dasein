package com.latticeengines.pls.entitymanager.impl;

import static org.testng.Assert.assertEquals;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.pls.Oauth2AccessToken;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.Oauth2AccessTokenEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.security.exposed.service.TenantService;

public class Oauth2AccessTokenEntityMgrImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private Oauth2AccessTokenEntityMgr oauth2AccessTokenEntityMgr;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        Tenant tenant1 = tenantService.findByTenantId("TENANT1");
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }
        Tenant tenant2 = tenantService.findByTenantId("TENANT2");
        if (tenant2 != null) {
            tenantService.discardTenant(tenant2);
        }
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        Tenant tenant1 = tenantService.findByTenantId("TENANT1");
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }
        Tenant tenant2 = tenantService.findByTenantId("TENANT2");
        if (tenant2 != null) {
            tenantService.discardTenant(tenant2);
        }
    }

    @Test(groups = "functional")
    public void testCreateToken() {
        String appId1 = "DUMMY_APP1";
        Tenant tenant1 = new Tenant();
        tenant1.setId("TENANT1");
        tenant1.setName("TENANT1");
        tenantEntityMgr.create(tenant1);
        assertEquals(oauth2AccessTokenEntityMgr.get(tenant1.getId(), appId1).getAccessToken(), "");

        String appId2 = null;
        Tenant tenant2 = new Tenant();
        tenant2.setId("TENANT2");
        tenant2.setName("TENANT2");
        tenantEntityMgr.create(tenant2);
        assertEquals(oauth2AccessTokenEntityMgr.get(tenant2.getId(), appId2).getAccessToken(), "");

        Oauth2AccessToken token1 = new Oauth2AccessToken();
        token1.setAccessToken("somevalue1");
        oauth2AccessTokenEntityMgr.createOrUpdate(token1, tenant1.getId(), appId1);
        assertEquals(oauth2AccessTokenEntityMgr.get(tenant1.getId(), appId1).getAccessToken(), "somevalue1");

        Oauth2AccessToken token2 = new Oauth2AccessToken();
        token2.setAccessToken("somevalue2");
        oauth2AccessTokenEntityMgr.createOrUpdate(token2, tenant2.getId(), appId2);
        assertEquals(oauth2AccessTokenEntityMgr.get(tenant2.getId(), appId2).getAccessToken(), "somevalue2");

        String appId3 = "";
        Oauth2AccessToken token3 = oauth2AccessTokenEntityMgr.get(tenant1.getId(), appId3);
        token3.setAccessToken("somevalue3");
        oauth2AccessTokenEntityMgr.createOrUpdate(token3, tenant1.getId(), appId3);
        assertEquals(oauth2AccessTokenEntityMgr.get(tenant1.getId(), appId3).getAccessToken(), "somevalue3");
    }
}
