package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Date;

import com.latticeengines.domain.exposed.metadata.MetadataSegmentProperty;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentPropertyName;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.SegmentEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class SegmentEntityMgrImplTestNG extends MetadataFunctionalTestNGBase {
    @Autowired
    private SegmentEntityMgr segmentEntityMgr;

    @Autowired
    private TableEntityMgr tableEntityMgr;

    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    private MetadataSegment segment;
    private DataCollection defaultDataCollection;

    private static final MetadataSegmentProperty METADATA_SEGMENT_PROPERTY_1 = new MetadataSegmentProperty();
    private static final MetadataSegmentProperty METADATA_SEGMENT_PROPERTY_2 = new MetadataSegmentProperty();

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(CUSTOMERSPACE1));
        defaultDataCollection = dataCollectionEntityMgr.createDataCollection(new ArrayList<>(), null, true);
        METADATA_SEGMENT_PROPERTY_1.setOption(MetadataSegmentPropertyName.NumAccounts.getName());
        METADATA_SEGMENT_PROPERTY_1.setValue("100");
        METADATA_SEGMENT_PROPERTY_2.setOption(MetadataSegmentPropertyName.NumContacts.getName());
        METADATA_SEGMENT_PROPERTY_2.setValue("200");
    }

    @Test(groups = "functional")
    public void createSegment() {
        segment = new MetadataSegment();
        segment.setName("Test");
        segment.setDisplayName("Test");
        segment.setDescription("Test Description");
        segment.setUpdated(new Date());
        segment.setCreated(new Date());
        segment.addSegmentProperty(METADATA_SEGMENT_PROPERTY_1);
        segment.addSegmentProperty(METADATA_SEGMENT_PROPERTY_2);
        segmentEntityMgr.createOrUpdate(segment);
    }

    @Test(groups = "functional", dependsOnMethods = "createSegment")
    public void updateSegment() {
        segment = new MetadataSegment();
        segment.setName("Test");
        segment.setDisplayName("Updated Test");
        segment.setDescription("Updated Test Description");
        segment.setUpdated(new Date());
        segment.setCreated(new Date());
        segment.addSegmentProperty(METADATA_SEGMENT_PROPERTY_1);
        segment.addSegmentProperty(METADATA_SEGMENT_PROPERTY_2);
        segment.setRestriction(new ConcreteRestriction(false, null, ComparisonType.EQUAL, null));
        segmentEntityMgr.createOrUpdate(segment);
    }

    @Test(groups = "functional", dependsOnMethods = "updateSegment")
    public void getSegment() {
        MetadataSegment retrieved = segmentEntityMgr.findByName("Test");
        assertEquals(retrieved.getName(), segment.getName());
        assertEquals(((ConcreteRestriction) retrieved.getRestriction()).getRelation(), ComparisonType.EQUAL);
        assertTrue(retrieved.getDataCollection().isDefault());
    }

    @Test(groups = "functional", dependsOnMethods = "getSegment")
    public void getSegmentWithExplicitQuerySource() {
        MetadataSegment retrieved = segmentEntityMgr.findByName(defaultDataCollection.getName(), "Test");
        assertEquals(retrieved.getName(), segment.getName());
        assertEquals(((ConcreteRestriction) retrieved.getRestriction()).getRelation(), ComparisonType.EQUAL);
        assertTrue(retrieved.getDataCollection().isDefault());
    }

    @Test(groups = "functional", dependsOnMethods = "getSegmentWithExplicitQuerySource")
    public void deleteSegment() {
        segmentEntityMgr.delete(segment);
        assertEquals(segmentEntityMgr.findAll().size(), 0);
    }
}
