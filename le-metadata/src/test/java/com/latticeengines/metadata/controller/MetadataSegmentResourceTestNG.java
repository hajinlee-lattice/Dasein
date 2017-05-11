package com.latticeengines.metadata.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Date;
import java.util.List;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentProperty;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentPropertyName;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.ColumnLookup;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;

public class MetadataSegmentResourceTestNG extends MetadataFunctionalTestNGBase {

    private static final MetadataSegment METADATA_SEGMENT = new MetadataSegment();
    private static final DataCollection DATA_COLLECTION = new DataCollection();

    private static final MetadataSegmentProperty METADATA_SEGMENT_PROPERTY_1 = new MetadataSegmentProperty();
    private static final MetadataSegmentProperty METADATA_SEGMENT_PROPERTY_2 = new MetadataSegmentProperty();

    private static final String METADATA_SEGMENT_NAME = "NAME";
    private static final String METADATA_SEGMENT_DISPLAY_NAME = "DISPLAY_NAME";
    private static final String METADATA_SEGMENT_DESCRIPTION = "SEGMENT_DESCRIPTION";
    private static final Date CREATED_UPDATED_DATE = new Date();
    private static final int NUM_ACCOUNTS = 100;
    private static final int NUM_CONTACTS = 200;

    private static final String BASE_URL_DATA_COLLECTION = "/metadata/customerspaces/%s/datacollections/";
    private static final String BASE_URL_METADATA_SEGMENTS = "/metadata/customerspaces/%s/segments/";

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        DATA_COLLECTION.setType(DataCollectionType.Segmentation);
        restTemplate.postForObject(String.format(getRestAPIHostPort() + BASE_URL_DATA_COLLECTION, customerSpace1),
                DATA_COLLECTION, DataCollection.class);
        METADATA_SEGMENT_PROPERTY_1.setOption(MetadataSegmentPropertyName.NumAccounts.getName());
        METADATA_SEGMENT_PROPERTY_1.setValue(Integer.toString(NUM_ACCOUNTS));
        METADATA_SEGMENT_PROPERTY_2.setOption(MetadataSegmentPropertyName.NumContacts.getName());
        METADATA_SEGMENT_PROPERTY_2.setValue(Integer.toString(NUM_CONTACTS));

        METADATA_SEGMENT.setName(METADATA_SEGMENT_NAME);
        METADATA_SEGMENT.setDisplayName(METADATA_SEGMENT_DISPLAY_NAME);
        METADATA_SEGMENT.setDescription(METADATA_SEGMENT_DESCRIPTION);
        METADATA_SEGMENT.setUpdated(CREATED_UPDATED_DATE);
        METADATA_SEGMENT.setCreated(CREATED_UPDATED_DATE);
        METADATA_SEGMENT.addSegmentProperty(METADATA_SEGMENT_PROPERTY_1);
        METADATA_SEGMENT.addSegmentProperty(METADATA_SEGMENT_PROPERTY_2);
        METADATA_SEGMENT.setRestriction(new ConcreteRestriction(false, new ColumnLookup(
                SchemaInterpretation.BucketedAccountMaster, "BUSINESS_NAME"), ComparisonType.EQUAL, null));
        METADATA_SEGMENT.setDataCollection(DATA_COLLECTION);
    }

    @Test(groups = "functional")
    public void createMetadataSegment_assertCreated() {
        restTemplate.postForObject(String.format(getRestAPIHostPort() + BASE_URL_METADATA_SEGMENTS, customerSpace1),
                METADATA_SEGMENT, MetadataSegment.class);

        MetadataSegment retrieved = restTemplate.getForObject(String.format(getRestAPIHostPort()
                + BASE_URL_METADATA_SEGMENTS + "/name/%s", customerSpace1, METADATA_SEGMENT_NAME),
                MetadataSegment.class);
        assertNotNull(retrieved);
        assertEquals(retrieved.getName(), METADATA_SEGMENT_NAME);
        assertEquals(retrieved.getDisplayName(), METADATA_SEGMENT_DISPLAY_NAME);
        assertEquals(retrieved.getMetadataSegmentProperties().size(), 2);
        assertEquals(retrieved.getSegmentPropertyBag().getInt(MetadataSegmentPropertyName.NumAccounts), NUM_ACCOUNTS);
        assertEquals(retrieved.getSegmentPropertyBag().getInt(MetadataSegmentPropertyName.NumContacts), NUM_CONTACTS);
        assertEquals(((ConcreteRestriction) retrieved.getRestriction()).getRelation(), ComparisonType.EQUAL);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "functional", dependsOnMethods = "createMetadataSegment_assertCreated")
    public void deleteMetadataSegment_assertDeleted() {
        restTemplate.delete(String.format(getRestAPIHostPort() + BASE_URL_METADATA_SEGMENTS + "/%s", customerSpace1,
                METADATA_SEGMENT_NAME));

        List<MetadataSegment> retrieved = restTemplate.getForObject(
                String.format(getRestAPIHostPort() + BASE_URL_METADATA_SEGMENTS + "/all", customerSpace1), List.class);
        assertEquals(retrieved.size(), 0);
    }
}