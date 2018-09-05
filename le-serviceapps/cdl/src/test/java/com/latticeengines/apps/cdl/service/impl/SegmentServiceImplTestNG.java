package com.latticeengines.apps.cdl.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.RatingEngineService;
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

public class SegmentServiceImplTestNG extends CDLFunctionalTestNGBase {

    private static final String RATING_ENGINE_NOTE = "This is a Rating Engine that covers North America market";
    private static final String CREATED_BY = "lattice@lattice-engines.com";

    private static final String UTF8_STRING = "股票/证券公司";

    @Inject
    private RatingEngineService ratingEngineService;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDummySegment();
    }

    @Test(groups = "functional")
    public void testUtf8String() {
        MetadataSegment segment = segmentService.findByName(testSegment.getName());
        assertNotNull(segment);
        assertNull(segment.getDescription());
        segment.setDescription(UTF8_STRING);
        segmentService.createOrUpdateSegment(segment);
        segment = segmentService.findByName(testSegment.getName());
        assertNotNull(segment);
        assertNotNull(segment.getDescription());
        assertEquals(segment.getDescription(), UTF8_STRING);
    }

    @Test(groups = "functional")
    public void testFindDependingAttributes() {
        List<MetadataSegment> segments = new ArrayList<>();
        segments.add(testSegment);

        List<AttributeLookup> attributeLookups = segmentService.findDependingAttributes(segments);
        assertNotNull(attributeLookups);
        assertEquals(attributeLookups.size(), 8);
    }

    @Test(groups = "functional", dependsOnMethods = "testFindDependingAttributes", enabled = true)
    public void testFindDependingSegments() {
        List<String> attributes = new ArrayList<>();
        attributes.add("Contact.CompanyName");

        List<MetadataSegment> segments = segmentService.findDependingSegments(attributes);
        assertNotNull(segments);
        assertEquals(segments.size(), 1);
        assertEquals(segments.get(0).getDisplayName(), SEGMENT_NAME);
    }

    @Test(groups = "functional", dependsOnMethods = "testFindDependingSegments", enabled = false)
    public void testCyclicDependency() {
        MetadataSegment segment1 = createSegment(SEGMENT_NAME + "1");
        MetadataSegment segment2 = createSegment(SEGMENT_NAME + "2");

        RatingEngine ratingEngine1 = createRatingEngine(segment2);
        RatingEngine ratingEngine2 = createRatingEngine(segment1);

        try {
            Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
        }

        setRestriction(segment2, ratingEngine2);

        boolean exception = false;
        try {
            segment1.setDisplayName("Test");
            setRestriction(segment1, ratingEngine1);
        } catch (Exception e) {
            exception = true;
            assertTrue(e instanceof LedpException);
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_40041);
        }
        assertTrue(exception);
    }

    protected MetadataSegment createSegment(String segmentName) {
        MetadataSegment segment = new MetadataSegment();
        segment.setDisplayName(segmentName);
        MetadataSegment createdSegment = segmentService.createOrUpdateSegment(segment);

        try {
            Thread.sleep(2 * 1000);
        } catch (InterruptedException e) {
        }

        return segmentService.findByName(createdSegment.getName());
    }

    protected RatingEngine createRatingEngine(MetadataSegment segment) {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setSegment(segment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setType(RatingEngineType.RULE_BASED);
        ratingEngine.setNote(RATING_ENGINE_NOTE);

        return ratingEngineService.createOrUpdate(ratingEngine);
    }

    protected void setRestriction(MetadataSegment segment, RatingEngine ratingEngine) {
        Restriction accountRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Rating, ratingEngine.getId()), Bucket.notNullBkt());
        segment.setAccountRestriction(accountRestriction);
        segmentService.createOrUpdateSegment(segment);
    }
}
