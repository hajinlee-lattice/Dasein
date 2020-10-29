package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.PlayService;
import com.latticeengines.apps.cdl.service.PlayTypeService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.apps.cdl.service.TalkingPointService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.cdl.TalkingPointDTO;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayStatus;
import com.latticeengines.domain.exposed.pls.PlayType;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;

public class PlayServiceImplFunctionalTestNG extends CDLFunctionalTestNGBase {

    private static final String SEGMENT_NAME = "segment";
    private static final String PLAY_SEGMENT_NAME = "Play Segment for Service DeployTests";
    private static final String CREATED_BY = "lattice@lattice-engines.com";
    private static final String TALKINGPOINT_CONTENT = "<p>Space={!Space}</p> <p>Hello&nbsp;{!PlaySolutionName}, I am&nbsp;{!ExpectedValue}</p> <p>Let's checkout&nbsp;{!Account.Website}, and DUNS={!Account.DUNS},</p> <p>in&nbsp;{!Account.LDC_City},&nbsp;{!Account.LDC_State}, {!Account.LDC_Country}</p>";

    private static final Logger log = LoggerFactory.getLogger(PlayServiceImplFunctionalTestNG.class);

    @Inject
    private SegmentService segmentService;

    @Inject
    private PlayService playService;

    @Inject
    private RatingEngineService ratingEngineService;

    @Inject
    private TalkingPointService talkingPointService;

    @Inject
    private PlayTypeService playTypeService;

    private RatingEngine ratingEngine1;
    private MetadataSegment playSegment;
    private Play play;
    private String playName;
    private List<PlayType> playTypes;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironmentWithDataCollection();

        MetadataSegment createdSegment = segmentService.createOrUpdateSegment(constructSegment(SEGMENT_NAME));
        MetadataSegment retrievedSegment = segmentService.findByName(
                createdSegment.getName());
        Assert.assertNotNull(retrievedSegment);
        log.info(String.format("Segment is %s", retrievedSegment));

        playSegment = segmentService.createOrUpdateSegment(constructSegment(PLAY_SEGMENT_NAME));
        playSegment = segmentService.findByName(playSegment.getName());
        Assert.assertNotNull(playSegment);
        log.info(String.format("Play Segment is %s", playSegment));

        ratingEngine1 = new RatingEngine();
        ratingEngine1.setSegment(retrievedSegment);
        ratingEngine1.setCreatedBy(CREATED_BY);
        ratingEngine1.setUpdatedBy(CREATED_BY);
        ratingEngine1.setType(RatingEngineType.RULE_BASED);
        ratingEngine1.setTenant(mainTestTenant);
        RatingEngine createdRatingEngine = ratingEngineService.createOrUpdate(ratingEngine1);
        Assert.assertNotNull(createdRatingEngine);
        ratingEngine1.setId(createdRatingEngine.getId());
        ratingEngine1.setPid(createdRatingEngine.getPid());
        playTypes = playTypeService.getAllPlayTypes(mainCustomerSpace);
        play = createDefaultPlay();
    }

    @Test(groups = "functional")
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

    @Test(groups = "functional", dependsOnMethods = {"testCreateAndGet"})
    public void testFindDependingAttributes() {
        createTalkingPoints();
        List<AttributeLookup> attributes = playService.findDependingAttributes(playService.getAllPlays());

        Assert.assertNotNull(attributes);
        Assert.assertEquals(attributes.size(), 5);
    }

    @Test(groups = "functional", dependsOnMethods = {"testFindDependingAttributes"})
    public void testFindDependingPlays() {
        List<String> attributes = new ArrayList<>();
        attributes.add("Account.DUNS");
        List<String> plays = playService.findDependantPlayDisplayNames(attributes);

        Assert.assertNotNull(plays);
        Assert.assertEquals(plays.size(), 1);
    }

    @Test(groups = "functional", dependsOnMethods = {"testFindDependingPlays"})
    public void testDelete() {
        Play retrievedPlay = playService.getPlayByName(playName, false);
        Assert.assertNotNull(retrievedPlay);
        Assert.assertEquals(retrievedPlay.getName(), playName);
        Assert.assertEquals(retrievedPlay.getPlayStatus(), PlayStatus.ACTIVE);
        Assert.assertEquals(retrievedPlay.getDeleted(), Boolean.FALSE);

        retrievedPlay.setPlayStatus(PlayStatus.INACTIVE);
        playService.createOrUpdate(retrievedPlay, mainCustomerSpace);

        retrievedPlay = playService.getPlayByName(playName, true);
        Assert.assertNotNull(retrievedPlay);
        Assert.assertEquals(retrievedPlay.getName(), playName);
        Assert.assertEquals(retrievedPlay.getDeleted(), Boolean.FALSE);

        playService.deleteByName(playName, false);
        List<Play> playList = playService.getAllPlays();
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), 0);

        retrievedPlay = playService.getPlayByName(playName, false);
        Assert.assertNull(retrievedPlay);

        retrievedPlay = playService.getPlayByName(playName, true);
        Assert.assertNotNull(retrievedPlay);
        Assert.assertEquals(retrievedPlay.getName(), playName);
        Assert.assertEquals(retrievedPlay.getDeleted(), Boolean.TRUE);

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
        Assert.assertEquals(retrievedPlay.getDeleted(), Boolean.TRUE);

        deletedPlayIds = playService.getAllDeletedPlayIds(true);
        Assert.assertNotNull(deletedPlayIds);
        Assert.assertEquals(deletedPlayIds.size(), 0);

        deletedPlayIds = playService.getAllDeletedPlayIds(false);
        Assert.assertNotNull(deletedPlayIds);
        Assert.assertEquals(deletedPlayIds.size(), 1);
        Assert.assertEquals(deletedPlayIds.get(0), retrievedPlay.getName());
    }

    @Test(groups = "functional", dependsOnMethods = {"testDelete"})
    public void testFindDependantPlaysAfterDeleting() {
        List<String> attributes = new ArrayList<>();
        attributes.add("Account.DUNS");
        List<String> playNames = playService.findDependantPlayDisplayNames(attributes);

        Assert.assertNotNull(playNames);
        Assert.assertEquals(playNames.size(), 0);
    }

    @Test(groups = "functional", dependsOnMethods = {"testFindDependantPlaysAfterDeleting"})
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
        Assert.assertEquals(retrievedPlay.getDeleted(), Boolean.FALSE);

        try {
            ratingEngineService.deleteById(ratingEngine1.getId(), false, CREATED_BY);
            retrievedPlay = playService.getPlayByName(playName, false);
            Assert.assertNull(retrievedPlay);

            retrievedPlay = playService.getPlayByName(playName, true);
            Assert.assertNotNull(retrievedPlay);
            Assert.assertEquals(retrievedPlay.getName(), playName);
            Assert.assertEquals(retrievedPlay.getDeleted(), Boolean.TRUE);
        } catch (LedpException ex) {
            Assert.assertEquals(ex.getCode(), LedpCode.LEDP_40042);
            retrievedPlay = playService.getPlayByName(playName, false);
            Assert.assertNotNull(retrievedPlay);
            Assert.assertEquals(retrievedPlay.getName(), playName);
            Assert.assertEquals(retrievedPlay.getDeleted(), Boolean.FALSE);

        }
    }

    @Test(groups = "deployment-app", dependsOnMethods = {"testDeleteViaRatingEngine"})
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
        Assert.assertEquals(retrievedPlay.getName(), playName);
        Assert.assertEquals(retrievedPlay.getDeleted(), Boolean.FALSE);
        Assert.assertEquals(retrievedPlay.getRatingEngine().getId(), ratingEngine1.getId());

        try {
            String reSegmentName = retrievedPlay.getTargetSegment().getName();
            segmentService.deleteSegmentByName(mainCustomerSpace, false, true);
            retrievedPlay = playService.getPlayByName(playName, false);
            Assert.assertNull(retrievedPlay);

            retrievedPlay = playService.getPlayByName(playName, true);
            Assert.assertNotNull(retrievedPlay);
            Assert.assertEquals(retrievedPlay.getName(), playName);
            Assert.assertEquals(retrievedPlay.getDeleted(), Boolean.TRUE);
        } catch (LedpException ex) {
            Assert.assertEquals(ex.getCode(), LedpCode.LEDP_40042);
            retrievedPlay = playService.getPlayByName(playName, false);
            Assert.assertNotNull(retrievedPlay);
            Assert.assertEquals(retrievedPlay.getName(), playName);
            Assert.assertEquals(retrievedPlay.getDeleted(), Boolean.FALSE);

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
        play.setDisplayName("FuntionalTestPlayName");
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
        talkingPointService.createOrUpdate(talkingPointDTOS);
    }

    protected MetadataSegment constructSegment(String segmentName) {
        MetadataSegment segment = new MetadataSegment();
        Restriction accountRestriction = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "LDC_Name"),
                Bucket.notNullBkt());
        segment.setAccountRestriction(accountRestriction);
        Bucket titleBkt = Bucket.valueBkt("Buyer");
        Restriction contactRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Contact, InterfaceName.Title.name()), titleBkt);
        segment.setContactRestriction(contactRestriction);
        segment.setDisplayName(segmentName);
        return segment;
    }
}
