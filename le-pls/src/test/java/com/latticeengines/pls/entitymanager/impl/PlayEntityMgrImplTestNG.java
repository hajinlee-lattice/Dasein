package com.latticeengines.pls.entitymanager.impl;

import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.PlayEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class PlayEntityMgrImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private PlayEntityMgr playEntityMgr;

    @Autowired
    private TenantService tenantService;

    private Play play;

    private final static String NAME = "playHard";
    private final static String DISPLAY_NAME = "playHarder";

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {

        setupTestEnvironmentWithGATenants(1);
        Tenant tenant1 = testBed.getTestTenants().get(0);
        MultiTenantContext.setTenant(tenant1);

        play = new Play();
        play.setName(NAME);
        play.setTimeStamp(new Date(System.currentTimeMillis()));
        play.setLastUpdatedTimestamp(new Date(System.currentTimeMillis()));
        play.setDisplayName(DISPLAY_NAME);
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
        List<Play> playList = playEntityMgr.findAll();
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), 1);
        playEntityMgr.deleteByName(NAME);
    }

}
