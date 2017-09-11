package com.latticeengines.pls.service.impl;

import java.util.List;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.service.SegmentService;
import com.latticeengines.pls.entitymanager.RatingEngineEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.PlayService;
import com.latticeengines.proxy.exposed.dante.TalkingPointProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class PlayServiceImplTestNG extends PlsFunctionalTestNGBase {

    private static final String SEGMENT_NAME = "segment";
    private static final String CREATED_BY = "lattice@lattice-engines.com";

    private static final Logger log = LoggerFactory.getLogger(PlayServiceImplTestNG.class);

    @Autowired
    private SegmentService segmentService;

    private RatingEngine ratingEngine1;
    private MetadataSegment segment;

    @Autowired
    private PlayService playService;

    @Autowired
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Autowired
    private TalkingPointProxy talkingPointProxy;

    private Play play;

    private Tenant tenant;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironmentWithGATenants(1);
        tenant = testBed.getTestTenants().get(0);
        MultiTenantContext.setTenant(tenant);

        segment = new MetadataSegment();
        segment.setAccountFrontEndRestriction(new FrontEndRestriction());
        segment.setDisplayName(SEGMENT_NAME);
        MetadataSegment createdSegment = segmentService
                .createOrUpdateSegment(CustomerSpace.parse(tenant.getId()).toString(), segment);
        MetadataSegment retrievedSegment = segmentService.findByName(CustomerSpace.parse(tenant.getId()).toString(),
                createdSegment.getName());
        Assert.assertNotNull(retrievedSegment);

        ratingEngine1 = new RatingEngine();
        ratingEngine1.setSegment(retrievedSegment);
        ratingEngine1.setCreatedBy(CREATED_BY);
        ratingEngine1.setType(RatingEngineType.RULE_BASED);
        RatingEngine createdRatingEngine = ratingEngineEntityMgr.createOrUpdateRatingEngine(ratingEngine1,
                tenant.getId());
        Assert.assertNotNull(createdRatingEngine);
        ratingEngine1.setId(createdRatingEngine.getId());
        play = createDefaultPlay();
    }

    @Test(groups = "functional")
    public void testCrud() {
        Play newPlay = playService.createOrUpdate(play, tenant.getId());
        assertPlay(newPlay);
        String playName = newPlay.getName();
        newPlay = playService.getPlayByName(playName);
        assertPlay(newPlay);
        newPlay = playService.getFullPlayByName(playName);
        assertPlay(newPlay);
        playService.deleteByName(playName);
        newPlay = playService.getPlayByName(playName);
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

        // cannot use other servers in a functional test
        // need to either change this test to a deployment test
        // or mock the proxy using mockit
        // or mock dante server using StandaloneHttpServer

        // List<TalkingPointDTO> tps = new ArrayList<>();
        // talkingPointProxy.createOrUpdate(tps, tenant.getId());

        return play;
    }

}
