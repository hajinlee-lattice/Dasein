package com.latticeengines.metadata.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Date;
import java.util.List;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.LogicalOperator;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;

public class MetadataSegmentResourceTestNG extends MetadataFunctionalTestNGBase {

    private static final MetadataSegment METADATA_SEGMENT = new MetadataSegment();
    private static final DataCollection DATA_COLLECTION = new DataCollection();

    private static final String METADATA_SEGMENT_NAME = "NAME";
    private static final String METADATA_SEGMENT_DISPLAY_NAME = "DISPLAY_NAME";
    private static final String METADATA_SEGMENT_DESCRIPTION = "SEGMENT_DESCRIPTION";
    private static final Date CREATED_UPDATED_DATE = new Date();

    private static final String BASE_URL_DATA_COLLECTION = "/metadata/customerspaces/%s/datacollections/";
    private static final String BASE_URL_METADATA_SEGMENTS = "/metadata/customerspaces/%s/segments/";

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        restTemplate.postForObject(String.format(getRestAPIHostPort() + BASE_URL_DATA_COLLECTION, customerSpace1),
                DATA_COLLECTION, DataCollection.class);
        METADATA_SEGMENT.setName(METADATA_SEGMENT_NAME);
        METADATA_SEGMENT.setDisplayName(METADATA_SEGMENT_DISPLAY_NAME);
        METADATA_SEGMENT.setDescription(METADATA_SEGMENT_DESCRIPTION);
        METADATA_SEGMENT.setUpdated(CREATED_UPDATED_DATE);
        METADATA_SEGMENT.setCreated(CREATED_UPDATED_DATE);
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, "BUSINESS_NAME").isNull().build();
        METADATA_SEGMENT.setAccountRestriction(restriction);
        METADATA_SEGMENT
                .setContactRestriction(Restriction.builder().let(BusinessEntity.Contact, "EMAIL").isNull().build());
        METADATA_SEGMENT.setAccountContactOperator(LogicalOperator.AND);
        METADATA_SEGMENT.setDataCollection(DATA_COLLECTION);
    }

    @Test(groups = "functional", enabled = false)
    public void createMetadataSegment_assertCreated() {
        restTemplate.postForObject(String.format(getRestAPIHostPort() + BASE_URL_METADATA_SEGMENTS, customerSpace1),
                METADATA_SEGMENT, MetadataSegment.class);
        MetadataSegment retrieved = restTemplate.getForObject(String.format(getRestAPIHostPort()
                + BASE_URL_METADATA_SEGMENTS + "/name/%s", customerSpace1, METADATA_SEGMENT_NAME),
                MetadataSegment.class);
        assertNotNull(retrieved);
        assertEquals(retrieved.getName(), METADATA_SEGMENT_NAME);
        assertEquals(retrieved.getAccountContactOperator(), LogicalOperator.AND);
        assertEquals(retrieved.getDisplayName(), METADATA_SEGMENT_DISPLAY_NAME);
        assertEquals(((ConcreteRestriction) retrieved.getAccountRestriction()).getRelation(), ComparisonType.EQUAL);
        assertEquals(((ConcreteRestriction) retrieved.getContactRestriction()).getRelation(), ComparisonType.EQUAL);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "functional", dependsOnMethods = "createMetadataSegment_assertCreated", enabled = false)
    public void deleteMetadataSegment_assertDeleted() {
        restTemplate.delete(String.format(getRestAPIHostPort() + BASE_URL_METADATA_SEGMENTS + "/%s", customerSpace1,
                METADATA_SEGMENT_NAME));

        List<MetadataSegment> retrieved = restTemplate.getForObject(
                String.format(getRestAPIHostPort() + BASE_URL_METADATA_SEGMENTS + "/all", customerSpace1), List.class);
        assertEquals(retrieved.size(), 0);
    }
}