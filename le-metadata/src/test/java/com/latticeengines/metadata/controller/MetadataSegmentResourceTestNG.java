package com.latticeengines.metadata.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentDTO;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;

public class MetadataSegmentResourceTestNG extends MetadataFunctionalTestNGBase {

    private static final MetadataSegment METADATA_SEGMENT = new MetadataSegment();
    private static final DataCollection DATA_COLLECTION = new DataCollection();

    private static final String METADATA_SEGMENT_NAME = "NAME";
    private static final String METADATA_SEGMENT_NAME_2 = "NAME";
    private static final String METADATA_SEGMENT_DISPLAY_NAME = "DISPLAY_NAME";
    private static final String METADATA_SEGMENT_DESCRIPTION = "SEGMENT_DESCRIPTION";
    private static final String METADATA_SEGMENT_CREATED_BY = "abc@lattice-engines.com";
    private static final Date CREATED_UPDATED_DATE = new Date();

    private static final String BASE_URL_DATA_COLLECTION = "/metadata/customerspaces/%s/datacollections/";
    private static final String BASE_URL_METADATA_SEGMENTS = "/metadata/customerspaces/%s/segments/";

    @Inject
    private SegmentProxy segmentProxy;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        METADATA_SEGMENT.setName(METADATA_SEGMENT_NAME);
        METADATA_SEGMENT.setDisplayName(METADATA_SEGMENT_DISPLAY_NAME);
        METADATA_SEGMENT.setDescription(METADATA_SEGMENT_DESCRIPTION);
        METADATA_SEGMENT.setUpdated(CREATED_UPDATED_DATE);
        METADATA_SEGMENT.setCreated(CREATED_UPDATED_DATE);
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, "BUSINESS_NAME").isNull().build();
        METADATA_SEGMENT.setAccountRestriction(restriction);
        METADATA_SEGMENT
                .setContactRestriction(Restriction.builder().let(BusinessEntity.Contact, "EMAIL").isNull().build());
        METADATA_SEGMENT.setDataCollection(DATA_COLLECTION);
        METADATA_SEGMENT.setCreatedBy(METADATA_SEGMENT_CREATED_BY);
    }

    @Test(groups = "functional")
    public void testCrud() {
        testCreate();
        testUpdate();
        testDuplicate();
        testDelete();
    }

    private void testCreate() {
        MetadataSegment retrieved = segmentProxy.createOrUpdateSegment(customerSpace1, METADATA_SEGMENT);
        assertNotNull(retrieved);
        assertEquals(retrieved.getName(), METADATA_SEGMENT_NAME);
        assertEquals(retrieved.getDisplayName(), METADATA_SEGMENT_DISPLAY_NAME);
        assertEquals(retrieved.getCreatedBy(), METADATA_SEGMENT_CREATED_BY);
        assertEquals(((ConcreteRestriction) retrieved.getAccountRestriction()).getRelation(), ComparisonType.IS_NULL);
        assertEquals(((ConcreteRestriction) retrieved.getContactRestriction()).getRelation(), ComparisonType.IS_NULL);
    }

    private void testUpdate() {
        MetadataSegment retrieved = segmentProxy.getMetadataSegmentByName(customerSpace1, METADATA_SEGMENT.getName());
        MetadataSegmentDTO retrievedDTO = segmentProxy.getMetadataSegmentWithPidByName(customerSpace1,
                METADATA_SEGMENT.getName());
        Assert.assertNotNull(retrievedDTO.getPrimaryKey());
        Assert.assertNotNull(retrievedDTO.getMetadataSegment());
        Assert.assertEquals(retrievedDTO.getMetadataSegment().getDisplayName(), retrieved.getDisplayName());
        System.out.println("retrievedDTO is " + retrievedDTO);
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, "BUSINESS_NAME").eq("Alphabet Inc.")
                .build();
        retrieved.setAccountRestriction(restriction);
        retrieved.setCreated(null);
        MetadataSegment updated = segmentProxy.createOrUpdateSegment(customerSpace1, retrieved);
        assertNotNull(updated);
        assertEquals(updated.getName(), METADATA_SEGMENT_NAME);
        assertEquals(updated.getDisplayName(), METADATA_SEGMENT_DISPLAY_NAME);
        assertEquals(((ConcreteRestriction) updated.getAccountRestriction()).getRelation(), ComparisonType.EQUAL);
        assertEquals(((ConcreteRestriction) updated.getContactRestriction()).getRelation(), ComparisonType.IS_NULL);
    }

    private void testDuplicate() {
        MetadataSegment retrieved = segmentProxy.getMetadataSegmentByName(customerSpace1, METADATA_SEGMENT.getName());
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, "BUSINESS_NAME").eq("Alphabet Inc.")
                .build();
        retrieved.setAccountRestriction(restriction);
        retrieved.setName(METADATA_SEGMENT_NAME_2);

        MetadataSegment duplicated = segmentProxy.createOrUpdateSegment(customerSpace1, retrieved);
        assertNotNull(duplicated);
        assertEquals(duplicated.getName(), METADATA_SEGMENT_NAME_2);
        assertEquals(duplicated.getDisplayName(), METADATA_SEGMENT_DISPLAY_NAME);
        assertEquals(((ConcreteRestriction) duplicated.getAccountRestriction()).getRelation(), ComparisonType.EQUAL);
        assertEquals(((ConcreteRestriction) duplicated.getContactRestriction()).getRelation(), ComparisonType.IS_NULL);
        Assert.assertTrue(duplicated.getCreated().after(CREATED_UPDATED_DATE));
    }

    private void testDelete() {
        segmentProxy.deleteSegmentByName(customerSpace1, METADATA_SEGMENT_NAME);
        segmentProxy.deleteSegmentByName(customerSpace1, METADATA_SEGMENT_NAME_2);
        List<MetadataSegment> retrieved = segmentProxy.getMetadataSegments(customerSpace1);
        assertEquals(retrieved.size(), 0);
    }
}