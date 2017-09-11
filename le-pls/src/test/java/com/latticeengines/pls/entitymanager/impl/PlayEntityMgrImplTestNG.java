package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
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
import com.latticeengines.pls.entitymanager.PlayEntityMgr;
import com.latticeengines.pls.entitymanager.RatingEngineEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class PlayEntityMgrImplTestNG extends PlsFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PlayEntityMgrImplTestNG.class);
    private final static String NEW_DISPLAY_NAME = "playHarder!";
    private final static String DESCRIPTION = "playHardest";
    private final static String SEGMENT_NAME = "segment";
    private final static String CREATED_BY = "lattice@lattice-engines.com";

    @Autowired
    private PlayEntityMgr playEntityMgr;

    @Autowired
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Autowired
    private TenantService tenantService;

    private Play play;

    @Autowired
    private SegmentService segmentService;

    private RatingEngine ratingEngine1;
    private RatingEngine ratingEngine2;
    private MetadataSegment segment;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {

        setupTestEnvironmentWithGATenants(1);
        Tenant tenant = testBed.getTestTenants().get(0);
        MultiTenantContext.setTenant(tenant);

        segment = new MetadataSegment();
        segment.setAccountFrontEndRestriction(new FrontEndRestriction());
        segment.setDisplayName(SEGMENT_NAME);
        MetadataSegment createdSegment = segmentService
                .createOrUpdateSegment(CustomerSpace.parse(tenant.getId()).toString(), segment);
        MetadataSegment retrievedSegment = segmentService.findByName(CustomerSpace.parse(tenant.getId()).toString(),
                createdSegment.getName());
        Assert.assertNotNull(retrievedSegment);
        log.info(String.format("Created metadata segment with name %s", retrievedSegment.getName()));

        ratingEngine1 = new RatingEngine();
        ratingEngine1.setSegment(retrievedSegment);
        ratingEngine1.setCreatedBy(CREATED_BY);
        ratingEngine1.setType(RatingEngineType.RULE_BASED);
        RatingEngine createdRatingEngine = ratingEngineEntityMgr.createOrUpdateRatingEngine(ratingEngine1,
                tenant.getId());
        Assert.assertNotNull(createdRatingEngine);
        ratingEngine1.setId(createdRatingEngine.getId());

        ratingEngine2 = new RatingEngine();
        ratingEngine2.setSegment(retrievedSegment);
        ratingEngine2.setCreatedBy(CREATED_BY);
        ratingEngine2.setType(RatingEngineType.RULE_BASED);
        createdRatingEngine = ratingEngineEntityMgr.createOrUpdateRatingEngine(ratingEngine2, tenant.getId());
        Assert.assertNotNull(createdRatingEngine);
        ratingEngine2.setId(createdRatingEngine.getId());

        play = new Play();
        play.setDescription(DESCRIPTION);
        play.setCreatedBy(CREATED_BY);
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setId(ratingEngine1.getId());
        play.setRatingEngine(ratingEngine);
        play.setTenant(tenant);
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
        Assert.assertNotNull(retrievedPlay.getDisplayName());
        Assert.assertNotNull(retrievedPlay.getRatingEngine());
        Assert.assertEquals(retrievedPlay.getRatingEngine().getId(), ratingEngine1.getId());
        Assert.assertNotNull(retrievedPlay.getCreated());
        Assert.assertNotNull(retrievedPlay.getUpdated());
        Assert.assertFalse(retrievedPlay.getExcludeItemsWithoutSalesforceId());

        retrievedPlay.setDescription(null);
        retrievedPlay.setDisplayName(NEW_DISPLAY_NAME);
        RatingEngine newRatingEngine = new RatingEngine();
        newRatingEngine.setId(ratingEngine2.getId());
        retrievedPlay.setRatingEngine(newRatingEngine);
        retrievedPlay.setExcludeItemsWithoutSalesforceId(true);

        System.out.println("ratingEngine 1 is " + ratingEngine1.getId());
        System.out.println("ratingEngine 2 is " + ratingEngine2.getId());

        playEntityMgr.createOrUpdatePlay(retrievedPlay);
        retrievedPlay = playEntityMgr.findByName(playName);
        Assert.assertNotNull(retrievedPlay);
        Assert.assertEquals(retrievedPlay.getName(), playName);
        Assert.assertEquals(retrievedPlay.getDescription(), DESCRIPTION);
        Assert.assertEquals(retrievedPlay.getDisplayName(), NEW_DISPLAY_NAME);
        Assert.assertNotNull(retrievedPlay.getDisplayName());
        Assert.assertNotNull(retrievedPlay.getRatingEngine());
        Assert.assertEquals(retrievedPlay.getRatingEngine().getId(), ratingEngine2.getId());
        Assert.assertTrue(retrievedPlay.getExcludeItemsWithoutSalesforceId());

        playList = playEntityMgr.findAll();
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), 1);

        playEntityMgr.deleteByName(playName);
        playList = playEntityMgr.findAll();
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), 0);
    }

}
