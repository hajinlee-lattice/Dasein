package com.latticeengines.pls.service.impl;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.List;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.MetadataSegmentService;
import com.latticeengines.pls.service.RatingEngineService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class RatingEngineServiceImplDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineServiceImplDeploymentTestNG.class);

    private static final String RATING_ENGINE_NAME = "Rating Engine";
    private static final String RATING_ENGINE_NOTE = "This is a Rating Engine that covers North America market";
    private static final String SEGMENT_NAME = "segment";
    private static final String CREATED_BY = "lattice@lattice-engines.com";

    @Autowired
    private MetadataSegmentService metadataSegmentService;

    @Autowired
    private RatingEngineService ratingEngineService;

    private RatingEngine ratingEngine;

    private MetadataSegment segment;

    @BeforeClass(groups = "deployment")
    public void setup() throws KeyManagementException, NoSuchAlgorithmException, IOException {
        setupTestEnvironmentWithOneTenant();
        MultiTenantContext.setTenant(mainTestTenant);
        mainTestTenant = testBed.getMainTestTenant();
        switchToSuperAdmin();
        segment = new MetadataSegment();
        segment.setFrontEndRestriction(new FrontEndRestriction());
        segment.setDisplayName(SEGMENT_NAME);
        MetadataSegment createdSegment = metadataSegmentService.createOrUpdateSegment(segment);
        Assert.assertNotNull(createdSegment);
        MetadataSegment retrievedSegment = metadataSegmentService.getSegmentByName(createdSegment.getName(), false);
        log.info(String.format("Created metadata segment with name %s", retrievedSegment.getName()));
        ratingEngine = new RatingEngine();
        ratingEngine.setSegment(retrievedSegment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setType(RatingEngineType.RULE_BASED);
    }

    @Test(groups = "deployment")
    public void testBasicOperations() {
        RatingEngine createdRatingEngine = ratingEngineService.createOrUpdate(ratingEngine, mainTestTenant.getId());
        Assert.assertNotNull(createdRatingEngine);
        Assert.assertNotNull(createdRatingEngine.getId());
        String id = createdRatingEngine.getId();
        Assert.assertNotNull(createdRatingEngine.getCreated());
        Date createdDate = createdRatingEngine.getCreated();
        Assert.assertNotNull(createdRatingEngine.getUpdated());
        Date updatedDate = createdRatingEngine.getUpdated();
        Assert.assertNull(createdRatingEngine.getDisplayName());
        Assert.assertNull(createdRatingEngine.getNote());
        Assert.assertEquals(createdRatingEngine.getType(), RatingEngineType.RULE_BASED);
        Assert.assertEquals(createdRatingEngine.getCreatedBy(), CREATED_BY);
        Assert.assertNotNull(createdRatingEngine.getRatingModels());
        System.out.println("size of getRatingModels() " + createdRatingEngine.getRatingModels().size());

        List<RatingEngine> ratingEngineList = ratingEngineService.getAllRatingEngines();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 1);
        Assert.assertEquals(id, ratingEngineList.get(0).getId());

        createdRatingEngine = ratingEngineService.getRatingEngineById(id);
        Assert.assertNotNull(createdRatingEngine);
        Assert.assertEquals(id, createdRatingEngine.getId());

        ratingEngine.setDisplayName(RATING_ENGINE_NAME);
        ratingEngine.setNote(RATING_ENGINE_NOTE);
        createdRatingEngine = ratingEngineService.createOrUpdate(ratingEngine, mainTestTenant.getId());
        Assert.assertEquals(RATING_ENGINE_NAME, createdRatingEngine.getDisplayName());
        Assert.assertEquals(RATING_ENGINE_NOTE, createdRatingEngine.getNote());
        Assert.assertTrue(createdRatingEngine.getUpdated().after(updatedDate));
        System.out.println("Created date is " + createdDate);
        System.out.println("The create date for the newly updated one is " + createdRatingEngine.getCreated());
        ratingEngineList = ratingEngineService.getAllRatingEngines();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 1);
        Assert.assertEquals(id, ratingEngineList.get(0).getId());

        ratingEngineService.deleteById(createdRatingEngine.getId());
        ratingEngineList = ratingEngineService.getAllRatingEngines();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 0);

        createdRatingEngine = ratingEngineService.getRatingEngineById(createdRatingEngine.getId());
        Assert.assertNull(createdRatingEngine);

    }

}
