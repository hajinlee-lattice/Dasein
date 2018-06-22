package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class SegmentServiceImplTestNG extends CDLFunctionalTestNGBase {

    private static final String RATING_ENGINE_NOTE = "This is a Rating Engine that covers North America market";
    private static final String CREATED_BY = "lattice@lattice-engines.com";

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @BeforeClass(groups = "functional")
    public void setup() { setupTestEnvironmentWithDummySegment(); }

    @Test(groups = "functional")
    public void testFindDependingAttributes() {
        List<MetadataSegment> segments = new ArrayList<>();
        segments.add(testSegment);

        List<AttributeLookup> attributeLookups = segmentService.findDependingAttributes(segments);
        assertNotNull(attributeLookups);
        assertEquals(attributeLookups.size(), 8);
    }

    @Test(groups = "functional", dependsOnMethods = "testFindDependingAttributes")
    public void testFindDependingSegments() {
        List<String> attributes = new ArrayList<>();
        attributes.add("Contact.CompanyName");

        List<MetadataSegment> segments = segmentService.findDependingSegments(mainCustomerSpace, attributes);
        assertNotNull(segments);
        assertEquals(segments.size(), 1);
        assertEquals(segments.get(0).getDisplayName(), SEGMENT_NAME);
    }

    @Test(groups = "functional", dependsOnMethods = "testFindDependingSegments")
    public void testCyclicDependency() {
        MetadataSegment segment1 = createSegment(SEGMENT_NAME + "1");
        MetadataSegment segment2 = createSegment(SEGMENT_NAME + "2");

        RatingEngine ratingEngine1 = createRatingEngine(segment2);
        RatingEngine ratingEngine2 = createRatingEngine(segment1);

        try {
            Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
        }

        setRestriction(segment1, ratingEngine1);
        setRestriction(segment2, ratingEngine2);

        boolean exception = false;
        try {
            segment1.setDisplayName("Test");
            segmentService.createOrUpdateSegment(mainTestTenant.getId(), segment1);
        } catch (Exception e) {
            exception = true;
            assertTrue(e instanceof LedpException);
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_40025);
        }
        assertTrue(exception);
    }

    protected MetadataSegment createSegment(String segmentName) {
        MetadataSegment segment = new MetadataSegment();
        segment.setDisplayName(segmentName);
        MetadataSegment createdSegment = segmentService.createOrUpdateSegment(mainTestTenant.getId(), segment);

        try {
            Thread.sleep(2 * 1000);
        } catch (InterruptedException e) {
        }

        return segmentService.findByName(mainTestTenant.getId(), createdSegment.getName());
    }

    protected RatingEngine createRatingEngine(MetadataSegment segment) {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setSegment(segment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setType(RatingEngineType.RULE_BASED);
        ratingEngine.setNote(RATING_ENGINE_NOTE);

        return ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(), ratingEngine);
    }

    protected void setRestriction(MetadataSegment segment, RatingEngine ratingEngine) {
        Restriction accountRestriction = new BucketRestriction(new AttributeLookup(BusinessEntity.Rating,
                RatingEngine.RATING_ENGINE_PREFIX + "_" + ratingEngine.getId()), Bucket.notNullBkt());
        segment.setAccountRestriction(accountRestriction);
        segmentService.createOrUpdateSegment(mainTestTenant.getId(), segment);
    }
}
