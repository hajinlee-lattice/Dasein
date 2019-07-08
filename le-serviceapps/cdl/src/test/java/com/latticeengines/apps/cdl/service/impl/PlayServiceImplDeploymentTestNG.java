package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.PlayService;
import com.latticeengines.apps.cdl.service.PlayTypeService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.domain.exposed.cdl.TalkingPointDTO;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayStatus;
import com.latticeengines.domain.exposed.pls.PlayType;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.cdl.TalkingPointProxy;

public class PlayServiceImplDeploymentTestNG extends CDLDeploymentTestNGBase {

    private static final String SEGMENT_NAME = "segment";
    private static final String PLAY_SEGMENT_NAME = "Play Segment for Service DeployTests";
    private static final String CREATED_BY = "lattice@lattice-engines.com";
    private static final String TALKINGPOINT_CONTENT = "<p>Space={!Space}</p> <p>Hello&nbsp;{!PlaySolutionName}, I am&nbsp;{!ExpectedValue}</p> <p>Let's checkout&nbsp;{!Account.Website}, and DUNS={!Account.DUNS},</p> <p>in&nbsp;{!Account.LDC_City},&nbsp;{!Account.LDC_State}, {!Account.LDC_Country}</p>";

    private static final Logger log = LoggerFactory.getLogger(PlayServiceImplDeploymentTestNG.class);

    @Value("${cdl.model.delete.propagate:false}")
    private Boolean shouldPropagateDelete;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private PlayService playService;

    @Inject
    private RatingEngineService ratingEngineService;

    @Inject
    private TalkingPointProxy talkingPointProxy;

    @Inject
    private PlayTypeService playTypeService;

    private RatingEngine ratingEngine1;
    private MetadataSegment playSegment;
    private Play play;
    private String playName;
    private List<PlayType> playTypes;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironment();

        MetadataSegment createdSegment = segmentProxy.createOrUpdateSegment(mainCustomerSpace,
                constructSegment(SEGMENT_NAME));
        MetadataSegment retrievedSegment = segmentProxy.getMetadataSegmentByName(mainCustomerSpace,
                createdSegment.getName());
        Assert.assertNotNull(retrievedSegment);
        log.info(String.format("Segment is %s", retrievedSegment));

        playSegment = segmentProxy.createOrUpdateSegment(mainCustomerSpace, constructSegment(PLAY_SEGMENT_NAME));
        playSegment = segmentProxy.getMetadataSegmentByName(mainCustomerSpace, playSegment.getName());
        Assert.assertNotNull(playSegment);
        log.info(String.format("Play Segment is %s", playSegment));

        ratingEngine1 = new RatingEngine();
        ratingEngine1.setSegment(retrievedSegment);
        ratingEngine1.setCreatedBy(CREATED_BY);
        ratingEngine1.setType(RatingEngineType.RULE_BASED);
        ratingEngine1.setTenant(mainTestTenant);
        RatingEngine createdRatingEngine = ratingEngineService.createOrUpdate(ratingEngine1);
        Assert.assertNotNull(createdRatingEngine);
        ratingEngine1.setId(createdRatingEngine.getId());
        ratingEngine1.setPid(createdRatingEngine.getPid());
        playTypes = playTypeService.getAllPlayTypes(mainCustomerSpace);
        play = createDefaultPlay();
    }

    @Test(groups = "deployment")
    public void testCreateAndGet() {
        Play newPlay = playService.createOrUpdate(play, mainTestTenant.getId());
        assertPlay(newPlay);
        playName = newPlay.getName();
        newPlay = playService.getPlayByName(playName, true);
        assertPlay(newPlay);
        newPlay = playService.getFullPlayByName(playName, true);
        assertPlay(newPlay);
        List<Play> plays = playService.getAllFullPlays(false, ratingEngine1.getId());
        Assert.assertNotNull(plays);
        Assert.assertEquals(plays.size(), 1);
        try {
            playService.getAllFullPlays(false, "someRandomString");
            Assert.fail("Should have thrown exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NullPointerException);
        }
    }

    @Test(groups = "deployment", dependsOnMethods = { "testCreateAndGet" })
    public void testFindDependingAttributes() {
        createTalkingPoints();
        List<AttributeLookup> attributes = playService.findDependingAttributes(playService.getAllPlays());

        Assert.assertNotNull(attributes);
        Assert.assertEquals(attributes.size(), 5);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testFindDependingAttributes" })
    public void testFindDependingPlays() {
        List<String> attributes = new ArrayList<>();
        attributes.add("Account.DUNS");
        List<Play> plays = playService.findDependingPalys(attributes);

        Assert.assertNotNull(plays);
        Assert.assertEquals(plays.size(), 1);
        assertPlay(plays.get(0));
    }

    @Test(groups = "deployment", dependsOnMethods = { "testFindDependingPlays" })
    public void testDelete() {
        Play retrievedPlay = playService.getPlayByName(playName, false);
        Assert.assertNotNull(retrievedPlay);
        Assert.assertNotNull(retrievedPlay);
        Assert.assertEquals(retrievedPlay.getName(), playName);
        Assert.assertNotNull(retrievedPlay.getDisplayName());
        Assert.assertNotNull(retrievedPlay.getRatingEngine());
        Assert.assertEquals(retrievedPlay.getPlayStatus(), PlayStatus.ACTIVE);
        Assert.assertEquals(retrievedPlay.getDeleted(), Boolean.FALSE);
        Assert.assertEquals(retrievedPlay.getIsCleanupDone(), Boolean.FALSE);
        Assert.assertEquals(retrievedPlay.getRatingEngine().getId(), ratingEngine1.getId());

        retrievedPlay.setPlayStatus(PlayStatus.INACTIVE);
        playService.createOrUpdate(retrievedPlay, mainCustomerSpace);

        retrievedPlay = playService.getPlayByName(playName, true);
        Assert.assertNotNull(retrievedPlay);
        Assert.assertNotNull(retrievedPlay);
        Assert.assertEquals(retrievedPlay.getName(), playName);
        Assert.assertNotNull(retrievedPlay.getDisplayName());
        Assert.assertNotNull(retrievedPlay.getRatingEngine());
        Assert.assertEquals(retrievedPlay.getPlayStatus(), PlayStatus.INACTIVE);
        Assert.assertEquals(retrievedPlay.getDeleted(), Boolean.FALSE);
        Assert.assertEquals(retrievedPlay.getIsCleanupDone(), Boolean.FALSE);
        Assert.assertEquals(retrievedPlay.getRatingEngine().getId(), ratingEngine1.getId());

        playService.deleteByName(playName, false);
        List<Play> playList = playService.getAllPlays();
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), 0);

        retrievedPlay = playService.getPlayByName(playName, false);
        Assert.assertNull(retrievedPlay);

        retrievedPlay = playService.getPlayByName(playName, true);
        Assert.assertNotNull(retrievedPlay);
        Assert.assertEquals(retrievedPlay.getName(), playName);
        Assert.assertNotNull(retrievedPlay.getDisplayName());
        Assert.assertNotNull(retrievedPlay.getRatingEngine());
        Assert.assertEquals(retrievedPlay.getPlayStatus(), PlayStatus.INACTIVE);
        Assert.assertEquals(retrievedPlay.getDeleted(), Boolean.TRUE);
        Assert.assertEquals(retrievedPlay.getIsCleanupDone(), Boolean.FALSE);
        Assert.assertEquals(retrievedPlay.getRatingEngine().getId(), ratingEngine1.getId());

        List<String> deletedPlayIds = playService.getAllDeletedPlayIds(true);
        Assert.assertNotNull(deletedPlayIds);
        Assert.assertEquals(deletedPlayIds.size(), 1);
        Assert.assertEquals(deletedPlayIds.get(0), retrievedPlay.getName());

        deletedPlayIds = playService.getAllDeletedPlayIds(false);
        Assert.assertNotNull(deletedPlayIds);
        Assert.assertEquals(deletedPlayIds.size(), 1);
        Assert.assertEquals(deletedPlayIds.get(0), retrievedPlay.getName());

        retrievedPlay.setIsCleanupDone(Boolean.TRUE);
        playService.createOrUpdate(retrievedPlay, mainTestTenant.getId());

        retrievedPlay = playService.getPlayByName(playName, true);
        Assert.assertNotNull(retrievedPlay);
        Assert.assertEquals(retrievedPlay.getName(), playName);
        Assert.assertNotNull(retrievedPlay.getDisplayName());
        Assert.assertNotNull(retrievedPlay.getRatingEngine());
        Assert.assertEquals(retrievedPlay.getPlayStatus(), PlayStatus.INACTIVE);
        Assert.assertEquals(retrievedPlay.getDeleted(), Boolean.TRUE);
        Assert.assertEquals(retrievedPlay.getIsCleanupDone(), Boolean.TRUE);
        Assert.assertEquals(retrievedPlay.getRatingEngine().getId(), ratingEngine1.getId());

        deletedPlayIds = playService.getAllDeletedPlayIds(true);
        Assert.assertNotNull(deletedPlayIds);
        Assert.assertEquals(deletedPlayIds.size(), 0);

        deletedPlayIds = playService.getAllDeletedPlayIds(false);
        Assert.assertNotNull(deletedPlayIds);
        Assert.assertEquals(deletedPlayIds.size(), 1);
        Assert.assertEquals(deletedPlayIds.get(0), retrievedPlay.getName());
    }

    @Test(groups = "deployment", dependsOnMethods = { "testDelete" })
    public void testDeleteViaRatingEngine() {
        Play newPlay = playService.createOrUpdate(createDefaultPlay(), mainTestTenant.getId());
        assertPlay(newPlay);
        playName = newPlay.getName();
        newPlay = playService.getPlayByName(playName, true);
        assertPlay(newPlay);
        newPlay = playService.getFullPlayByName(playName, true);
        assertPlay(newPlay);
        List<Play> plays = playService.getAllFullPlays(false, ratingEngine1.getId());
        Assert.assertNotNull(plays);
        Assert.assertEquals(plays.size(), 1);

        Play retrievedPlay = playService.getPlayByName(playName, false);
        Assert.assertNotNull(retrievedPlay);
        Assert.assertNotNull(retrievedPlay);
        Assert.assertEquals(retrievedPlay.getName(), playName);
        Assert.assertNotNull(retrievedPlay.getDisplayName());
        Assert.assertNotNull(retrievedPlay.getRatingEngine());
        Assert.assertEquals(retrievedPlay.getPlayStatus(), PlayStatus.ACTIVE);
        Assert.assertEquals(retrievedPlay.getDeleted(), Boolean.FALSE);
        Assert.assertEquals(retrievedPlay.getIsCleanupDone(), Boolean.FALSE);
        Assert.assertEquals(retrievedPlay.getRatingEngine().getId(), ratingEngine1.getId());

        try {
            ratingEngineService.deleteById(ratingEngine1.getId(), false, CREATED_BY);
            if (shouldPropagateDelete != Boolean.TRUE) {
                Assert.fail("Should not be able to delete rating engine if non-deleted play exists");
            } else {
                retrievedPlay = playService.getPlayByName(playName, false);
                Assert.assertNull(retrievedPlay);

                retrievedPlay = playService.getPlayByName(playName, true);
                Assert.assertNotNull(retrievedPlay);
                Assert.assertEquals(retrievedPlay.getName(), playName);
                Assert.assertNotNull(retrievedPlay.getDisplayName());
                Assert.assertNotNull(retrievedPlay.getRatingEngine());
                Assert.assertEquals(retrievedPlay.getPlayStatus(), PlayStatus.ACTIVE);
                Assert.assertEquals(retrievedPlay.getDeleted(), Boolean.TRUE);
                Assert.assertEquals(retrievedPlay.getIsCleanupDone(), Boolean.FALSE);
                Assert.assertEquals(retrievedPlay.getRatingEngine().getId(), ratingEngine1.getId());
            }
        } catch (LedpException ex) {
            if (shouldPropagateDelete != Boolean.TRUE) {
                Assert.assertEquals(ex.getCode(), LedpCode.LEDP_40042);
                retrievedPlay = playService.getPlayByName(playName, false);
                Assert.assertNotNull(retrievedPlay);
                Assert.assertEquals(retrievedPlay.getName(), playName);
                Assert.assertNotNull(retrievedPlay.getDisplayName());
                Assert.assertNotNull(retrievedPlay.getRatingEngine());
                Assert.assertEquals(retrievedPlay.getPlayStatus(), PlayStatus.ACTIVE);
                Assert.assertEquals(retrievedPlay.getDeleted(), Boolean.FALSE);
                Assert.assertEquals(retrievedPlay.getIsCleanupDone(), Boolean.FALSE);
                Assert.assertEquals(retrievedPlay.getRatingEngine().getId(), ratingEngine1.getId());
            } else {
                Assert.fail("Should have been able to delete rating engine even "
                        + "if non-deleted play exists as it would have first soft deleted plays");
            }

        }
    }

    @Test(groups = "deployment", dependsOnMethods = { "testDeleteViaRatingEngine" })
    public void testDeleteViaSegment() {
        Play newPlay = playService.getPlayByName(playName, true);
        assertPlay(newPlay);
        newPlay = playService.getFullPlayByName(playName, true);
        assertPlay(newPlay);
        List<Play> plays = playService.getAllFullPlays(false, ratingEngine1.getId());
        Assert.assertNotNull(plays);
        Assert.assertEquals(plays.size(), 1);

        Play retrievedPlay = playService.getPlayByName(playName, false);
        Assert.assertNotNull(retrievedPlay);
        Assert.assertNotNull(retrievedPlay);
        Assert.assertEquals(retrievedPlay.getName(), playName);
        Assert.assertNotNull(retrievedPlay.getDisplayName());
        Assert.assertNotNull(retrievedPlay.getRatingEngine());
        Assert.assertEquals(retrievedPlay.getPlayStatus(), PlayStatus.ACTIVE);
        Assert.assertEquals(retrievedPlay.getDeleted(), Boolean.FALSE);
        Assert.assertEquals(retrievedPlay.getIsCleanupDone(), Boolean.FALSE);
        Assert.assertEquals(retrievedPlay.getRatingEngine().getId(), ratingEngine1.getId());

        try {
            String reSegmentName = retrievedPlay.getTargetSegment().getName();
            segmentProxy.deleteSegmentByName(mainCustomerSpace, reSegmentName, true);
            if (shouldPropagateDelete != Boolean.TRUE) {
                Assert.fail("Should not be able to delete segment if non-deleted rating engine exists in play");
            } else {
                retrievedPlay = playService.getPlayByName(playName, false);
                Assert.assertNull(retrievedPlay);

                retrievedPlay = playService.getPlayByName(playName, true);
                Assert.assertNotNull(retrievedPlay);
                Assert.assertEquals(retrievedPlay.getName(), playName);
                Assert.assertNotNull(retrievedPlay.getDisplayName());
                Assert.assertNotNull(retrievedPlay.getRatingEngine());
                Assert.assertEquals(retrievedPlay.getPlayStatus(), PlayStatus.ACTIVE);
                Assert.assertEquals(retrievedPlay.getDeleted(), Boolean.TRUE);
                Assert.assertEquals(retrievedPlay.getIsCleanupDone(), Boolean.FALSE);
                Assert.assertEquals(retrievedPlay.getRatingEngine().getId(), ratingEngine1.getId());
            }
        } catch (LedpException ex) {
            if (shouldPropagateDelete != Boolean.TRUE) {
                Assert.assertEquals(ex.getCode(), LedpCode.LEDP_40042);
                retrievedPlay = playService.getPlayByName(playName, false);
                Assert.assertNotNull(retrievedPlay);
                Assert.assertEquals(retrievedPlay.getName(), playName);
                Assert.assertNotNull(retrievedPlay.getDisplayName());
                Assert.assertNotNull(retrievedPlay.getRatingEngine());
                Assert.assertEquals(retrievedPlay.getPlayStatus(), PlayStatus.ACTIVE);
                Assert.assertEquals(retrievedPlay.getDeleted(), Boolean.FALSE);
                Assert.assertEquals(retrievedPlay.getIsCleanupDone(), Boolean.FALSE);
                Assert.assertEquals(retrievedPlay.getRatingEngine().getId(), ratingEngine1.getId());
            } else {
                Assert.fail("Should have been able to delete segment even "
                        + "if non-deleted play exists as it would have first soft deleted plays");
            }

        }
    }

    private void assertPlay(Play play) {
        Assert.assertNotNull(play);
        Assert.assertEquals(play.getCreatedBy(), CREATED_BY);
        Assert.assertNotNull(play.getRatingEngine());
        Assert.assertNotNull(play.getTargetSegment());
        Assert.assertEquals(play.getTargetSegment().getDisplayName(), PLAY_SEGMENT_NAME);
        Assert.assertNotNull(play.getName());
        Assert.assertNotNull(play.getPid());
        log.info(String.format("play is %s", play.toString()));
    }

    private Play createDefaultPlay() {
        Play play = new Play();
        play.setDisplayName("DeploymentTestPlayName");
        play.setCreatedBy(CREATED_BY);
        play.setUpdatedBy(CREATED_BY);
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setId(ratingEngine1.getId());
        play.setRatingEngine(ratingEngine);
        MetadataSegment targetSegment = new MetadataSegment();
        targetSegment.setName(playSegment.getName());
        play.setTargetSegment(targetSegment);
        play.setTenant(mainTestTenant);
        play.setPlayType(playTypes.get(0));
        // cannot use other servers in a functional test
        // need to either change this test to a deployment test
        // or mock the proxy using mockit
        // or mock dante server using StandaloneHttpServer

        // List<TalkingPointDTO> tps = new ArrayList<>();
        // talkingPointProxy.createOrUpdateByNameAndStorageType(tps,
        // tenant.getId());

        return play;
    }

    private void createTalkingPoints() {
        TalkingPointDTO talkingPointDTO = new TalkingPointDTO();
        talkingPointDTO.setName("TalkingPoint_Name");
        talkingPointDTO.setPlayName(playName);
        talkingPointDTO.setTitle("TalkingPoint_Title");
        talkingPointDTO.setContent(TALKINGPOINT_CONTENT);
        talkingPointDTO.setOffset(1);
        talkingPointDTO.setCreated(new Date());
        talkingPointDTO.setUpdated(new Date());
        List<TalkingPointDTO> talkingPointDTOS = new ArrayList<>();
        talkingPointDTOS.add(talkingPointDTO);
        talkingPointProxy.createOrUpdate(mainCustomerSpace, talkingPointDTOS);
    }
}
