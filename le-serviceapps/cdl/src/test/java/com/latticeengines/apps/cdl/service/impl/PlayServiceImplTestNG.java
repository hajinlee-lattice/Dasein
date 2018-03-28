package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.inject.Inject;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.PlayService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.multitenant.TalkingPoint;
import com.latticeengines.domain.exposed.multitenant.TalkingPointDTO;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;
import com.latticeengines.proxy.exposed.dante.TalkingPointProxy;

public class PlayServiceImplTestNG extends CDLFunctionalTestNGBase {

    private static final String CREATED_BY = "lattice@lattice-engines.com";
    private static final String TALKINGPOINT_CONTENT = "<p>Space={!Space}</p> <p>Hello&nbsp;{!PlaySolutionName}, I am&nbsp;{!ExpectedValue}</p> <p>Let's checkout&nbsp;{!Account.Website}, and DUNS={!Account.DUNS},</p> <p>in&nbsp;{!Account.LDC_City},&nbsp;{!Account.LDC_State}, {!Account.LDC_Country}</p>";

    private static final Logger log = LoggerFactory.getLogger(PlayServiceImplTestNG.class);

    @Inject
    private RatingEngineService ratingEngineService;

    @Inject
    private PlayService playService;

    @Inject
    private TalkingPointProxy talkingPointProxy;

    @BeforeClass(groups = "functional")
    public void setup() {
        LogManager.getLogger(BaseRestApiProxy.class).setLevel(Level.DEBUG);
        setupTestEnvironmentWithDummySegment();
        createDefaultPlayWithTalkingPoints();
    }

    @Test(groups = "functional")
    public void testFindDependingAttributes() {
        List<AttributeLookup> attributes = playService.findDependingAttributes(playService.getAllPlays());

        Assert.assertNotNull(attributes);
        Assert.assertEquals(attributes.size(), 5);
    }

    @Test(groups = "functional")
    public void testFindDependingPalys() {
        List<String> attributes = new ArrayList<>();
        attributes.add("Account.DUNS");
        List<Play> plays = playService.findDependingPalys(attributes);

        Assert.assertNotNull(plays);
        Assert.assertEquals(plays.size(), 1);
        assertPlay(plays.get(0));
    }

    private void assertPlay(Play play) {
        Assert.assertNotNull(play);
        Assert.assertEquals(play.getCreatedBy(), CREATED_BY);
        Assert.assertNotNull(play.getRatingEngine());
        log.info(String.format("play is %s", play.toString()));
    }

    private void createDefaultPlayWithTalkingPoints() {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setSegment(testSegment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setType(RatingEngineType.RULE_BASED);
        ratingEngine.setTenant(mainTestTenant);
        ratingEngine = ratingEngineService.createOrUpdate(ratingEngine, mainCustomerSpace);
        Assert.assertNotNull(ratingEngine);

        Play play = new Play();
        play.setCreatedBy(CREATED_BY);
        play.setRatingEngine(ratingEngine);
        play.setTenant(mainTestTenant);
        play = playService.createOrUpdate(play, mainCustomerSpace);
        Assert.assertNotNull(play);

        TalkingPoint talkingPoint = new TalkingPoint();
        talkingPoint.setName("TalkingPoint_Name");
        talkingPoint.setPlay(play);
        talkingPoint.setTitle("TalkingPoint_Title");
        talkingPoint.setContent(TALKINGPOINT_CONTENT);
        talkingPoint.setOffset(1);
        talkingPoint.setCreated(new Date());
        talkingPoint.setUpdated(new Date());
        List<TalkingPointDTO> talkingPointDTOS = new ArrayList<>();
        talkingPointDTOS.add(new TalkingPointDTO(talkingPoint));
        talkingPointProxy.createOrUpdate(talkingPointDTOS, mainCustomerSpace);
    }
}
