package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.PlayService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.multitenant.TalkingPointDTO;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.proxy.exposed.dante.TalkingPointProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;

public class PlayServiceImplDeploymentTestNG extends CDLDeploymentTestNGBase {

    private static final String SEGMENT_NAME = "segment";
    private static final String CREATED_BY = "lattice@lattice-engines.com";
    private static final String TALKINGPOINT_CONTENT = "<p>Space={!Space}</p> <p>Hello&nbsp;{!PlaySolutionName}, I am&nbsp;{!ExpectedValue}</p> <p>Let's checkout&nbsp;{!Account.Website}, and DUNS={!Account.DUNS},</p> <p>in&nbsp;{!Account.LDC_City},&nbsp;{!Account.LDC_State}, {!Account.LDC_Country}</p>";

    private static final Logger log = LoggerFactory.getLogger(PlayServiceImplDeploymentTestNG.class);

    @Inject
    private SegmentProxy segmentProxy;

    private RatingEngine ratingEngine1;

    @Inject
    private PlayService playService;

    @Inject
    private RatingEngineService ratingEngineService;

    @Inject
    private TalkingPointProxy talkingPointProxy;

    private Play play;
    private String playName;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironment();

        MetadataSegment createdSegment = segmentProxy.createOrUpdateSegment(
                CustomerSpace.parse(mainTestTenant.getId()).toString(), constructSegment(SEGMENT_NAME));
        MetadataSegment retrievedSegment = segmentProxy.getMetadataSegmentByName(
                CustomerSpace.parse(mainTestTenant.getId()).toString(), createdSegment.getName());
        Assert.assertNotNull(retrievedSegment);
        log.info(String.format("Segment is %s", retrievedSegment));

        ratingEngine1 = new RatingEngine();
        ratingEngine1.setSegment(retrievedSegment);
        ratingEngine1.setCreatedBy(CREATED_BY);
        ratingEngine1.setType(RatingEngineType.RULE_BASED);
        ratingEngine1.setTenant(mainTestTenant);
        RatingEngine createdRatingEngine = ratingEngineService.createOrUpdate(ratingEngine1, mainTestTenant.getId());
        Assert.assertNotNull(createdRatingEngine);
        ratingEngine1.setId(createdRatingEngine.getId());
        ratingEngine1.setPid(createdRatingEngine.getPid());
        play = createDefaultPlay();
    }

    @Test(groups = "deployment")
    public void testCreateAndGet() {
        Play newPlay = playService.createOrUpdate(play, mainTestTenant.getId());
        assertPlay(newPlay);
        playName = newPlay.getName();
        newPlay = playService.getPlayByName(playName);
        assertPlay(newPlay);
        newPlay = playService.getFullPlayByName(playName);
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
    public void testFindDependingPalys() {
        List<String> attributes = new ArrayList<>();
        attributes.add("Account.DUNS");
        List<Play> plays = playService.findDependingPalys(attributes);

        Assert.assertNotNull(plays);
        Assert.assertEquals(plays.size(), 1);
        assertPlay(plays.get(0));
    }

    @Test(groups = "deployment", dependsOnMethods = { "testFindDependingPalys" })
    public void testDelete() {
        playService.deleteByName(playName);
        Play newPlay = playService.getPlayByName(playName);
        Assert.assertNull(newPlay);
        List<Play> plays = playService.getAllPlays();
        Assert.assertNotNull(plays);
        Assert.assertEquals(plays.size(), 0);
    }

    private void assertPlay(Play play) {
        Assert.assertNotNull(play);
        Assert.assertEquals(play.getCreatedBy(), CREATED_BY);
        Assert.assertNotNull(play.getRatingEngine());
        log.info(String.format("play is %s", play.toString()));
    }

    private Play createDefaultPlay() {
        Play play = new Play();
        play.setCreatedBy(CREATED_BY);
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setId(ratingEngine1.getId());
        play.setRatingEngine(ratingEngine);
        play.setTenant(mainTestTenant);
        // cannot use other servers in a functional test
        // need to either change this test to a deployment test
        // or mock the proxy using mockit
        // or mock dante server using StandaloneHttpServer

        // List<TalkingPointDTO> tps = new ArrayList<>();
        // talkingPointProxy.createOrUpdate(tps, tenant.getId());

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
        talkingPointProxy.createOrUpdate(talkingPointDTOS, mainCustomerSpace);
    }
}
