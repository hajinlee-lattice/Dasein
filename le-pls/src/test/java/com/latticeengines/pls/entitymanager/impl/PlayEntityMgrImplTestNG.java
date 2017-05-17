package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.PlayEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.service.TenantService;

public class PlayEntityMgrImplTestNG extends PlsFunctionalTestNGBaseDeprecated {

    @Autowired
    private PlayEntityMgr playEntityMgr;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    private Play play;

    private final static String NAME = "playHard";
    private final static String DISPLAY_NAME = "playHarder";

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        Tenant tenant1 = tenantService.findByTenantId("TENANT1");
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }
        tenant1 = new Tenant();
        tenant1.setId("TENANT1");
        tenant1.setName("TENANT1");
        tenantEntityMgr.create(tenant1);
        setupSecurityContext(tenant1);

        play = new Play();
        play.setName(NAME);
        play.setDisplayName(DISPLAY_NAME);
        play.setVisible(true);
        play.setTenant(tenant1);
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        Tenant tenant1 = tenantService.findByTenantId("testTenant1");
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }
    }

    @Test(groups = "functional")
    public void testBasicOperations() {
        playEntityMgr.create(play);

        Play retreivedPlay = playEntityMgr.findByName(NAME);
        Assert.assertEquals(retreivedPlay.getName(), NAME);
        Assert.assertEquals(retreivedPlay.getDisplayName(), DISPLAY_NAME);
        Assert.assertNotNull(retreivedPlay);
        List<Play> playList = playEntityMgr.findAllVisible();
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), 1);
        playList = playEntityMgr.findAll();
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), 1);
        playEntityMgr.deleteByName(NAME);
    }

}
