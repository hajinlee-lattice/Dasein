package com.latticeengines.pls.entitymanager.impl;

import java.util.Date;
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
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.service.SegmentService;
import com.latticeengines.pls.entitymanager.RatingEngineEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class RatingEngineEntityMgrImplTestNG extends PlsFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineEntityMgrImplTestNG.class);

    private static final String RATING_ENGINE_NAME = "Rating Engine";
    private static final String RATING_ENGINE_NOTE = "This is a Rating Engine that covers North America market";
    private static final String SEGMENT_NAME = "segment";
    private static final String CREATED_BY = "lattice@lattice-engines.com";

    @Autowired
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Autowired
    private SegmentService segmentService;

    @Autowired
    private TenantService tenantService;

    private RatingEngine ratingEngine;
    private String ratingEngineId;

    private MetadataSegment segment;

    private Tenant tenant;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {

        setupTestEnvironmentWithGATenants(1);
        tenant = testBed.getTestTenants().get(0);
        MultiTenantContext.setTenant(tenant);

        segment = new MetadataSegment();
        segment.setFrontEndRestriction(new FrontEndRestriction());
        segment.setDisplayName(SEGMENT_NAME);
        MetadataSegment createdSegment = segmentService
                .createOrUpdateSegment(CustomerSpace.parse(tenant.getId()).toString(), segment);
        MetadataSegment retrievedSegment = segmentService.findByName(CustomerSpace.parse(tenant.getId()).toString(),
                createdSegment.getName());
        Assert.assertNotNull(retrievedSegment);
        log.info(String.format("Created metadata segment with name %s", retrievedSegment.getName()));
        ratingEngine = new RatingEngine();
        ratingEngine.setSegment(retrievedSegment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setType(RatingEngineType.RULE_BASED);

    }

    @Test(groups = "functional")
    public void testBasicOperations() {
        RatingEngine createdRatingEngine = ratingEngineEntityMgr.createOrUpdateRatingEngine(ratingEngine,
                tenant.getId());
        Assert.assertNotNull(createdRatingEngine);
        ratingEngineId = createdRatingEngine.getId();
        createdRatingEngine = ratingEngineEntityMgr.findById(createdRatingEngine.getId());
        Assert.assertNotNull(createdRatingEngine);
        Assert.assertEquals(ratingEngineId, createdRatingEngine.getId());
        Assert.assertNotNull(createdRatingEngine.getCreated());
        Date createdDate = createdRatingEngine.getCreated();
        Assert.assertNotNull(createdRatingEngine.getUpdated());
        Date updatedDate = createdRatingEngine.getUpdated();
        Assert.assertNull(createdRatingEngine.getDisplayName());
        Assert.assertNull(createdRatingEngine.getNote());
        Assert.assertEquals(createdRatingEngine.getType(), RatingEngineType.RULE_BASED);
        Assert.assertEquals(createdRatingEngine.getCreatedBy(), CREATED_BY);
        Assert.assertNotNull(createdRatingEngine.getRatingModels());

        List<RatingEngine> ratingEngineList = ratingEngineEntityMgr.findAll();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 1);
        Assert.assertEquals(ratingEngineId, ratingEngineList.get(0).getId());

        ratingEngine.setDisplayName(RATING_ENGINE_NAME);
        ratingEngine.setNote(RATING_ENGINE_NOTE);
        createdRatingEngine = ratingEngineEntityMgr.createOrUpdateRatingEngine(ratingEngine, mainTestTenant.getId());
        Assert.assertEquals(RATING_ENGINE_NAME, createdRatingEngine.getDisplayName());
        Assert.assertEquals(RATING_ENGINE_NOTE, createdRatingEngine.getNote());
        Assert.assertTrue(createdRatingEngine.getUpdated().after(updatedDate));
        System.out.println("Created date is " + createdDate);
        System.out.println("The create date for the newly updated one is " + createdRatingEngine.getCreated());
        ratingEngineList = ratingEngineEntityMgr.findAll();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 1);
        Assert.assertEquals(ratingEngineId, ratingEngineList.get(0).getId());

        ratingEngineEntityMgr.deleteById(ratingEngineId);
        ratingEngineList = ratingEngineEntityMgr.findAll();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 0);

        createdRatingEngine = ratingEngineEntityMgr.findById(createdRatingEngine.getId());
        Assert.assertNull(createdRatingEngine);

    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        if (tenant != null) {
            tenantService.discardTenant(tenant);
        }
    }

}
