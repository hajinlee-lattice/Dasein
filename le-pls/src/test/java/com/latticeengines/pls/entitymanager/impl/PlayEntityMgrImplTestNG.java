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

    private final static String DISPLAY_NAME = "playHarder";
    private final static String NEW_DISPLAY_NAME = "playHarder!";
    private final static String DESCRIPTION = "playHardest";
    private final static String SEGMENT_NAME = "segment";

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {

        setupTestEnvironmentWithGATenants(1);
        Tenant tenant1 = testBed.getTestTenants().get(0);
        MultiTenantContext.setTenant(tenant1);

        play = new Play();
        play.setTimeStamp(new Date(System.currentTimeMillis()));
        play.setLastUpdatedTimestamp(new Date(System.currentTimeMillis()));
        play.setDisplayName(DISPLAY_NAME);
        play.setDescription(DESCRIPTION);
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
        playEntityMgr.createOrUpdatePlay(play);
        List<Play> playList = playEntityMgr.findAll();
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), 1);
        Play play1 = playList.get(0);
        String playName = play1.getName();
        System.out.println(String.format("play1 has name %s", playName));
        Play retrievedPlay = playEntityMgr.findByName(playName);
        Assert.assertNotNull(retrievedPlay);
        Assert.assertEquals(retrievedPlay.getName(), play1.getName());
        Assert.assertEquals(retrievedPlay.getDescription(), DESCRIPTION);
        Assert.assertEquals(retrievedPlay.getDisplayName(), DISPLAY_NAME);

        retrievedPlay.setDescription(null);
        retrievedPlay.setDisplayName(NEW_DISPLAY_NAME);
        retrievedPlay.setSegmentName(SEGMENT_NAME);
        playEntityMgr.createOrUpdatePlay(retrievedPlay);
        retrievedPlay = playEntityMgr.findByName(playName);
        Assert.assertNotNull(retrievedPlay);
        Assert.assertEquals(retrievedPlay.getName(), playName);
        Assert.assertEquals(retrievedPlay.getDescription(), DESCRIPTION);
        Assert.assertEquals(retrievedPlay.getDisplayName(), NEW_DISPLAY_NAME);
        Assert.assertEquals(retrievedPlay.getSegmentName(), SEGMENT_NAME);

        playList = playEntityMgr.findAll();
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), 1);

        playEntityMgr.deleteByName(playName);
        playList = playEntityMgr.findAll();
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), 0);
    }

}
