package com.latticeengines.pls.service.impl;

import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.PlayService;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class PlayServiceImplTestNG extends PlsFunctionalTestNGBase {

    private static final String PLAY_NAME = "play";
    private static final String PLAY_DISPLAY_NAME = "play hard";
    private static final String SEGMENT_NAME = "segment";

    @Autowired
    private PlayService playService;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    private Play play;

    private Tenant tenant1;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        tenant1 = tenantService.findByTenantId("TENANT1");

        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }
        tenant1 = new Tenant();
        tenant1.setId("TENANT1");
        tenant1.setName("TENANT1");
        tenantEntityMgr.create(tenant1);
        MultiTenantContext.setTenant(tenant1);

        play = createDefaultPlay();
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        tenant1 = tenantService.findByTenantId("TENANT1");
        tenantService.discardTenant(tenant1);
    }

    @Test(groups = "functional")
    public void testCrud() {
        Play newPlay = playService.createOrUpdate(play, tenant1.getId());
        assertPlay(newPlay);
        String playName = newPlay.getName();
        newPlay = playService.getPlayByName(playName);
        assertPlay(newPlay);
        newPlay = playService.getPlayByName(playName);
        assertPlay(newPlay);
        playService.deleteByName(playName);
        newPlay = playService.getPlayByName(playName);
        Assert.assertNull(newPlay);
    }

    private void assertPlay(Play play) {
        Assert.assertNotNull(play);
        Assert.assertEquals(play.getName(), PLAY_NAME);
        Assert.assertEquals(play.getDisplayName(), PLAY_DISPLAY_NAME);
        Assert.assertEquals(play.getSegmentName(), SEGMENT_NAME);
    }

    private Play createDefaultPlay() {
        Play play = new Play();
        MetadataSegment segment = new MetadataSegment();
        segment.setName(SEGMENT_NAME);
        segment.setDisplayName(SEGMENT_NAME);
        play.setName(PLAY_NAME);
        play.setDisplayName(PLAY_DISPLAY_NAME);
        play.setSegment(segment);
        play.setSegmentName(SEGMENT_NAME);

        return play;
    }

}
