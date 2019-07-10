package com.latticeengines.apps.cdl.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.RatingEngineEntityMgr;
import com.latticeengines.apps.cdl.service.PlayTypeService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayStatus;
import com.latticeengines.domain.exposed.pls.PlayType;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.service.TenantService;

public class PlayEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PlayEntityMgrImplTestNG.class);
    private static final String DISPLAY_NAME = "playHard!";
    private static final String NEW_DISPLAY_NAME = "playHarder!";
    private static final String DESCRIPTION = "playHardest";
    private static final String CREATED_BY = "lattice@lattice-engines.com";
    private static final String UPDATED_BY = "updated@lattice-engines.com";
    private static final String PLAY_SEGMENT_NAME = "PlayTargetSegment";

    @Autowired
    private PlayEntityMgr playEntityMgr;

    @Autowired
    private PlayTypeService playTypeService;

    @Autowired
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Autowired
    private TenantService tenantService;

    private Play play;
    private RatingEngine ratingEngine1;
    private RatingEngine ratingEngine2;
    private MetadataSegment playTargetSegment;

    private Play retrievedPlay;
    private String playName;
    private List<PlayType> types;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {

        setupTestEnvironmentWithDummySegment();

        ratingEngine1 = new RatingEngine();
        ratingEngine1.setSegment(testSegment);
        ratingEngine1.setCreatedBy(CREATED_BY);
        ratingEngine1.setUpdatedBy(CREATED_BY);
        ratingEngine1.setType(RatingEngineType.RULE_BASED);
        ratingEngine1.setId(UUID.randomUUID().toString());
        RatingEngine createdRatingEngine = ratingEngineEntityMgr.createRatingEngine(ratingEngine1);
        Assert.assertNotNull(createdRatingEngine);
        ratingEngine1.setId(createdRatingEngine.getId());
        ratingEngine1.setPid(createdRatingEngine.getPid());

        ratingEngine2 = new RatingEngine();
        ratingEngine2.setSegment(testSegment);
        ratingEngine2.setCreatedBy(CREATED_BY);
        ratingEngine1.setUpdatedBy(CREATED_BY);
        ratingEngine2.setType(RatingEngineType.RULE_BASED);
        ratingEngine2.setId(UUID.randomUUID().toString());
        createdRatingEngine = ratingEngineEntityMgr.createRatingEngine(ratingEngine2);
        Assert.assertNotNull(createdRatingEngine);
        ratingEngine2.setId(createdRatingEngine.getId());
        ratingEngine2.setPid(createdRatingEngine.getPid());

        playTargetSegment = createMetadataSegment(PLAY_SEGMENT_NAME);
        Assert.assertNotNull(playTargetSegment);
        Assert.assertNotNull(playTargetSegment.getPid());

        types = playTypeService.getAllPlayTypes(mainCustomerSpace);
        play = new Play();
        play.setDisplayName(DISPLAY_NAME);
        play.setDescription(DESCRIPTION);
        play.setPlayType(types.get(0));
        play.setCreatedBy(CREATED_BY);
        play.setUpdatedBy(CREATED_BY);
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setId(ratingEngine1.getId());
        play.setRatingEngine(ratingEngine);
        play.setPlayStatus(PlayStatus.INACTIVE);
        play.setTenant(mainTestTenant);
        play.setName(UUID.randomUUID().toString());
        play.setTargetSegment(playTargetSegment);
    }

    @Test(groups = "functional")
    public void testCreate() {
        playEntityMgr.createPlay(play);
        List<Play> playList = playEntityMgr.findAll();
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), 1);
        Play play1 = playList.get(0);
        playName = play1.getName();
        log.info(String.format("play1 has name %s", playName));

        assertPlayTargetSegment(play1);
        retrievedPlay = playEntityMgr.getPlayByName(playName, true);
        assertPlayTargetSegment(retrievedPlay);
    }

    private void assertPlayTargetSegment(Play testPlay) {
        assertNotNull(testPlay.getTargetSegment());
        assertNotNull(testPlay.getTargetSegment().getPid());
        assertNotNull(testPlay.getTargetSegment().getDisplayName());
        assertEquals(testPlay.getTargetSegment().getDisplayName(), PLAY_SEGMENT_NAME);
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreate" })
    public void testFind() {
        List<Play> plays = playEntityMgr.findAllByRatingEnginePid(ratingEngine1.getPid());
        Assert.assertNotNull(plays);
        Assert.assertEquals(plays.size(), 1);
        assertPlayTargetSegment(plays.get(0));
        plays = playEntityMgr.findAllByRatingEnginePid(ratingEngine2.getPid());
        Assert.assertNotNull(plays);
        Assert.assertEquals(plays.size(), 0);

        plays = playEntityMgr.findByRatingEngineAndPlayStatusIn(ratingEngine1, Arrays.asList(PlayStatus.INACTIVE));
        Assert.assertNotNull(plays);
        Assert.assertEquals(plays.size(), 1);
        plays = playEntityMgr.findByRatingEngineAndPlayStatusIn(ratingEngine1, Arrays.asList(PlayStatus.ACTIVE));
        Assert.assertEquals(plays.size(), 0);
    }

    @Test(groups = "functional", dependsOnMethods = { "testFind" })
    public void testUpdate() {
        retrievedPlay.setDescription(null);
        retrievedPlay.setDisplayName(NEW_DISPLAY_NAME);
        retrievedPlay.setPlayStatus(PlayStatus.INACTIVE);
        retrievedPlay.setUpdatedBy(UPDATED_BY);
        RatingEngine newRatingEngine = new RatingEngine();
        newRatingEngine.setId(ratingEngine2.getId());
        retrievedPlay.setRatingEngine(newRatingEngine);

        log.info("ratingEngine 1 is " + ratingEngine1.getId());
        log.info("ratingEngine 2 is " + ratingEngine2.getId());

        playEntityMgr.updatePlay(retrievedPlay, playEntityMgr.getPlayByName(retrievedPlay.getName(), false));
        retrievedPlay = playEntityMgr.getPlayByName(playName, true);
        Assert.assertNotNull(retrievedPlay);
        Assert.assertEquals(retrievedPlay.getName(), playName);
        Assert.assertEquals(retrievedPlay.getDescription(), DESCRIPTION);
        Assert.assertEquals(retrievedPlay.getDisplayName(), NEW_DISPLAY_NAME);
        Assert.assertNotNull(retrievedPlay.getDisplayName());
        Assert.assertNotNull(retrievedPlay.getRatingEngine());
        Assert.assertEquals(retrievedPlay.getPlayStatus(), PlayStatus.INACTIVE);
        Assert.assertEquals(retrievedPlay.getDeleted(), Boolean.FALSE);
        Assert.assertEquals(retrievedPlay.getIsCleanupDone(), Boolean.FALSE);
        Assert.assertEquals(retrievedPlay.getRatingEngine().getId(), ratingEngine2.getId());
        Assert.assertEquals(retrievedPlay.getUpdatedBy(), UPDATED_BY);

        assertPlayTargetSegment(retrievedPlay);

        List<Play> playList = playEntityMgr.findAll();
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), 1);

        playList = playEntityMgr.findByRatingEngineAndPlayStatusIn(ratingEngine2, Arrays.asList(PlayStatus.INACTIVE));
        Assert.assertEquals(playList.size(), 1);
        playList = playEntityMgr.findByRatingEngineAndPlayStatusIn(ratingEngine1, Arrays.asList(PlayStatus.INACTIVE));
        Assert.assertEquals(playList.size(), 0);

        long playTypeCount = playEntityMgr.countByPlayTypePid(types.get(0).getPid());
        Assert.assertEquals(playTypeCount, 1);
        playTypeCount = playEntityMgr.countByPlayTypePid(types.get(1).getPid());
        Assert.assertEquals(playTypeCount, 0);
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdate" })
    public void testDelete() {
        List<Play> playList = playEntityMgr.findAll();
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), 1);

        retrievedPlay = playEntityMgr.getPlayByName(playName, false);
        Assert.assertNotNull(retrievedPlay);

        retrievedPlay = playEntityMgr.getPlayByName(playName, true);
        Assert.assertNotNull(retrievedPlay);

        playEntityMgr.deleteByName(playName, false);
        playList = playEntityMgr.findAll();
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), 0);

        retrievedPlay = playEntityMgr.getPlayByName(playName, false);
        Assert.assertNull(retrievedPlay);

        retrievedPlay = playEntityMgr.getPlayByName(playName, true);
        Assert.assertNotNull(retrievedPlay);
        Assert.assertEquals(retrievedPlay.getName(), playName);
        Assert.assertEquals(retrievedPlay.getDescription(), DESCRIPTION);
        Assert.assertEquals(retrievedPlay.getDisplayName(), NEW_DISPLAY_NAME);
        Assert.assertNotNull(retrievedPlay.getDisplayName());
        Assert.assertNotNull(retrievedPlay.getRatingEngine());
        Assert.assertEquals(retrievedPlay.getPlayStatus(), PlayStatus.INACTIVE);
        Assert.assertEquals(retrievedPlay.getDeleted(), Boolean.TRUE);
        Assert.assertEquals(retrievedPlay.getIsCleanupDone(), Boolean.FALSE);
        Assert.assertEquals(retrievedPlay.getRatingEngine().getId(), ratingEngine2.getId());

        List<String> deletedPlayIds = playEntityMgr.getAllDeletedPlayIds(true);
        Assert.assertNotNull(deletedPlayIds);
        Assert.assertEquals(deletedPlayIds.size(), 1);
        Assert.assertEquals(deletedPlayIds.get(0), retrievedPlay.getName());

        deletedPlayIds = playEntityMgr.getAllDeletedPlayIds(false);
        Assert.assertNotNull(deletedPlayIds);
        Assert.assertEquals(deletedPlayIds.size(), 1);
        Assert.assertEquals(deletedPlayIds.get(0), retrievedPlay.getName());

        retrievedPlay.setIsCleanupDone(Boolean.TRUE);
        playEntityMgr.updatePlay(retrievedPlay, playEntityMgr.getPlayByName(retrievedPlay.getName(), true));

        retrievedPlay = playEntityMgr.getPlayByName(playName, true);
        Assert.assertNotNull(retrievedPlay);
        Assert.assertEquals(retrievedPlay.getName(), playName);
        Assert.assertEquals(retrievedPlay.getDescription(), DESCRIPTION);
        Assert.assertEquals(retrievedPlay.getDisplayName(), NEW_DISPLAY_NAME);
        Assert.assertNotNull(retrievedPlay.getDisplayName());
        Assert.assertNotNull(retrievedPlay.getRatingEngine());
        Assert.assertEquals(retrievedPlay.getPlayStatus(), PlayStatus.INACTIVE);
        Assert.assertEquals(retrievedPlay.getDeleted(), Boolean.TRUE);
        Assert.assertEquals(retrievedPlay.getIsCleanupDone(), Boolean.TRUE);
        Assert.assertEquals(retrievedPlay.getRatingEngine().getId(), ratingEngine2.getId());

        deletedPlayIds = playEntityMgr.getAllDeletedPlayIds(true);
        Assert.assertNotNull(deletedPlayIds);
        Assert.assertEquals(deletedPlayIds.size(), 0);

        deletedPlayIds = playEntityMgr.getAllDeletedPlayIds(false);
        Assert.assertNotNull(deletedPlayIds);
        Assert.assertEquals(deletedPlayIds.size(), 1);
        Assert.assertEquals(deletedPlayIds.get(0), retrievedPlay.getName());
    }

    @AfterClass(groups = "functional")
    public void teardown() {
        Tenant tenant1 = tenantService.findByTenantId("testTenant1");
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }
    }
}
