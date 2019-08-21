package com.latticeengines.apps.cdl.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.log4testng.Logger;

import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.KryoUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Restriction;

public class SegmentServiceImplTestNG extends CDLFunctionalTestNGBase {

    private Logger log = Logger.getLogger(getClass());

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

    @Test(groups = "functional", dependsOnMethods = "testFindDependingSegments", enabled = true)
    public void testCyclicDependency() {
        MetadataSegment segment1 = createSegment(SEGMENT_NAME + "1");
        MetadataSegment segment2 = createSegment(SEGMENT_NAME + "2");

        RatingEngine ratingEngine1 = createRatingEngine(segment2);
        RatingEngine ratingEngine2 = createRatingEngine(segment1);

        try {
            Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
            // ignore
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

    @Test(groups = "functional", dependsOnMethods = "testCyclicDependency", enabled = true)
    public void testDeleteSegmentByName() {
        String segmentName = testSegment.getName();
        boolean exception = false;
        try {
            log.info("Retrieving segmentName: " + segmentName);
            boolean res = segmentService.deleteSegmentByName(segmentName, true, true);
            log.info("Retrieving segment: " + res);
            assertTrue(res);
        } catch (Exception e) {
            Assert.fail("Exception: " + e.getMessage());
        }
    }

    @Test(groups = "functional")
    public void testInvalidBucket() {
        Restriction b1 = createBucketRestriction( //
                BusinessEntity.Account, "Attr1", ComparisonType.EQUAL, 6);
        Restriction b2 = createBucketRestriction( //
                BusinessEntity.Account, "Attr2", ComparisonType.GTE_AND_LTE, 1, 2);
        Restriction accountRestriction = Restriction.builder().and(b1, b2).build();

        Restriction b3 = createBucketRestriction( //
                BusinessEntity.Contact, "Attr1", ComparisonType.EQUAL, 4);
        Restriction b4 = createBucketRestriction( //
                BusinessEntity.Contact, "Attr2", ComparisonType.IN_COLLECTION, 1, 2);
        Restriction contactRestriction = Restriction.builder().and(b3, b4).build();

        MetadataSegment segment = new MetadataSegment();
        segment.setDisplayName("Valid Buckets");
        segment.setAccountRestriction(accountRestriction);
        segment.setContactRestriction(contactRestriction);

        MetadataSegment newSegment = segmentService.createOrUpdateSegment(segment);
        Assert.assertNotNull(newSegment);

        Restriction b1c = cloneRestriction(b1);
        ((BucketRestriction) b1c).getBkt().setValues(null);
        ((LogicalRestriction) accountRestriction).getRestrictions().set(0, b1c);
        segment = createSegment("Invalid Bucket b1", accountRestriction, contactRestriction);
        try {
            segmentService.createOrUpdateSegment(segment);
            Assert.fail("Should throw exception.");
        } catch (LedpException e) {
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_40057);
            Assert.assertTrue(e.getMessage().contains("Account.Attr1"), e.getMessage());
        }

        Restriction b2c = cloneRestriction(b2);
        ((BucketRestriction) b2c).getBkt().setValues(Collections.singletonList(1));
        ((LogicalRestriction) accountRestriction).getRestrictions().set(1, b2c);
        segment = createSegment("Invalid Bucket b2", accountRestriction, contactRestriction);
        try {
            segmentService.createOrUpdateSegment(segment);
            Assert.fail("Should throw exception.");
        } catch (LedpException e) {
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_40057);
            Assert.assertTrue(e.getMessage().contains("Account.Attr1,Account.Attr2"), e.getMessage());
        }
        ((LogicalRestriction) accountRestriction).getRestrictions().set(0, b1);
        ((LogicalRestriction) accountRestriction).getRestrictions().set(1, b2);

        /* Disable this check for now because we need to relax validation until UI doesn't set "" as value in array
        Restriction b3c = cloneRestriction(b3);
        ((BucketRestriction) b3c).getBkt().setValues(Collections.emptyList());
        ((LogicalRestriction) contactRestriction).getRestrictions().set(0, b3c);
        segment = createSegment("Invalid Bucket b3", accountRestriction, contactRestriction);
        try {
            segmentService.createOrUpdateSegment(segment);
            Assert.fail("Should throw exception.");
        } catch (LedpException e) {
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_40057);
            Assert.assertTrue(e.getMessage().contains("Contact.Attr1"), e.getMessage());
        }
        */

        Restriction b4c = cloneRestriction(b4);
        ((BucketRestriction) b4c).getBkt().setValues(Collections.emptyList());
        ((LogicalRestriction) contactRestriction).getRestrictions().set(1, b4c);
        segment = createSegment("Invalid Bucket b4", accountRestriction, contactRestriction);
        try {
            segmentService.createOrUpdateSegment(segment);
            Assert.fail("Should throw exception.");
        } catch (LedpException e) {
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_40057);
            Assert.assertTrue(e.getMessage().contains("Contact.Attr1,Contact.Attr2"), e.getMessage());
        }
        ((LogicalRestriction) contactRestriction).getRestrictions().set(0, b3);
        ((LogicalRestriction) contactRestriction).getRestrictions().set(1, b4);
    }

    private MetadataSegment createSegment(String displayName, Restriction account, Restriction contact) {
        MetadataSegment segment = new MetadataSegment();
        segment.setDisplayName(displayName);
        segment.setAccountRestriction(account);
        segment.setContactRestriction(contact);
        return segment;
    }

    private MetadataSegment createSegment(String segmentName) {
        MetadataSegment segment = new MetadataSegment();
        segment.setDisplayName(segmentName);
        MetadataSegment createdSegment = segmentService.createOrUpdateSegment(segment);

        try {
            Thread.sleep(2 * 1000);
        } catch (InterruptedException e) {
            // ignore
        }

        return segmentService.findByName(createdSegment.getName());
    }

    protected RatingEngine createRatingEngine(MetadataSegment segment) {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setSegment(segment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setUpdatedBy(CREATED_BY);
        ratingEngine.setType(RatingEngineType.RULE_BASED);
        ratingEngine.setNote(RATING_ENGINE_NOTE);

        return ratingEngineService.createOrUpdate(ratingEngine);
    }

    private void setRestriction(MetadataSegment segment, RatingEngine ratingEngine) {
        Restriction accountRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Rating, ratingEngine.getId()), Bucket.notNullBkt());
        segment.setAccountRestriction(accountRestriction);
        segmentService.createOrUpdateSegment(segment);
    }

    private Restriction cloneRestriction(Restriction r1) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        KryoUtils.write(bos, r1);
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        return KryoUtils.read(bis, Restriction.class);
    }
}
