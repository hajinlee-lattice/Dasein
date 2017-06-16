package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentProperty;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentPropertyName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.ColumnLookup;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.metadata.entitymgr.SegmentEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.functionalframework.DataCollectionFunctionalTestNGBase;
import com.latticeengines.metadata.service.DataCollectionService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class MetadataSegmentEntityMgrImplTestNG extends DataCollectionFunctionalTestNGBase {
    @Autowired
    private SegmentEntityMgr segmentEntityMgr;

    @Autowired
    private TableEntityMgr tableEntityMgr;

    @Autowired
    private DataCollectionService dataCollectionService;

    private static final String DATA_COLLECTION_NAME = "SegmentTestCollection";
    private static final String SEGMENT_NAME = "SEGMENT_NAME";

    private static final String SEGMENT_DISPLAY_NAME = "SEGMENT_DISPLAY_NAME";
    private static final String UPDATED_DISPLAY_SEGMENT_NAME = "UPDATED_DISPLAY_SEGMENT_NAME";
    private static final String SEGMENT_DESCRITION = "SEGMENT_DESCRIPTION";
    private static final String UPDATED_SEGMENT_DESCRIPTION = "UPDATED_SEGMENT_DESCRIPTION";
    private static final MetadataSegment METADATA_SEGMENT = new MetadataSegment();
    private static final MetadataSegmentProperty METADATA_SEGMENT_PROPERTY_1 = new MetadataSegmentProperty();
    private static final MetadataSegmentProperty METADATA_SEGMENT_PROPERTY_2 = new MetadataSegmentProperty();

    private Attribute arbitraryAttribute;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        createSegmentInOtherTenant();

        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace1));
        Table table = new Table();
        table.setName(TABLE1);
        addTableToCollection(table);

        METADATA_SEGMENT_PROPERTY_1.setOption(MetadataSegmentPropertyName.NumAccounts.getName());
        METADATA_SEGMENT_PROPERTY_1.setValue("100");
        METADATA_SEGMENT_PROPERTY_2.setOption(MetadataSegmentPropertyName.NumContacts.getName());
        METADATA_SEGMENT_PROPERTY_2.setValue("200");

        arbitraryAttribute = getTablesInCollection().get(0).getAttributes().get(5);
    }

    private void createSegmentInOtherTenant() {
        MetadataSegment otherSegment = new MetadataSegment();
        otherSegment.setName("Other");
        otherSegment.setDisplayName("Other");
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace2));
        segmentEntityMgr.createOrUpdate(otherSegment);
    }

    @Test(groups = "functional", enabled = false)
    public void createSegment() {
        METADATA_SEGMENT.setName(SEGMENT_NAME);
        METADATA_SEGMENT.setDisplayName(SEGMENT_DISPLAY_NAME);
        METADATA_SEGMENT.setDescription(SEGMENT_DESCRITION);
        METADATA_SEGMENT.setUpdated(new Date());
        METADATA_SEGMENT.setCreated(new Date());
        METADATA_SEGMENT.addSegmentProperty(METADATA_SEGMENT_PROPERTY_1);
        METADATA_SEGMENT.addSegmentProperty(METADATA_SEGMENT_PROPERTY_2);
        METADATA_SEGMENT.setRestriction(new ConcreteRestriction( //
                false, //
                new ColumnLookup(SchemaInterpretation.valueOf(arbitraryAttribute.getTable().getInterpretation()),
                        arbitraryAttribute.getName()), //
                ComparisonType.EQUAL, //
                null));
        segmentEntityMgr.createOrUpdate(METADATA_SEGMENT);

        MetadataSegment retrieved = segmentEntityMgr.findByName(SEGMENT_NAME);
        assertNotNull(retrieved);
        assertEquals(retrieved.getName(), METADATA_SEGMENT.getName());
        assertEquals(retrieved.getDisplayName(), METADATA_SEGMENT.getDisplayName());
        assertEquals(((ConcreteRestriction) retrieved.getRestriction()).getRelation(), ComparisonType.EQUAL);
        assertEquals(METADATA_SEGMENT.getDataCollection().getType(), DataCollectionType.Segmentation);
        assertEquals(retrieved.getProperties().size(), 2);
        assertEquals(retrieved.getSegmentPropertyBag().getInt(MetadataSegmentPropertyName.NumAccounts), 100);
        assertEquals(retrieved.getSegmentPropertyBag().getInt(MetadataSegmentPropertyName.NumContacts), 200);
        assertEquals(retrieved.getAttributeDependencies().size(), 1);
    }

    @Test(groups = "functional", dependsOnMethods = "createSegment", enabled = false)
    public void updateSegment() {
        MetadataSegment UPDATED_SEGMENT = new MetadataSegment();
        UPDATED_SEGMENT.setName(SEGMENT_NAME);
        UPDATED_SEGMENT.setDisplayName(UPDATED_DISPLAY_SEGMENT_NAME);
        UPDATED_SEGMENT.setDescription(UPDATED_SEGMENT_DESCRIPTION);
        UPDATED_SEGMENT.setUpdated(new Date());
        UPDATED_SEGMENT.setCreated(new Date());
        UPDATED_SEGMENT.addSegmentProperty(copyFromExistingSegmentProperty(METADATA_SEGMENT_PROPERTY_1));
        UPDATED_SEGMENT.addSegmentProperty(copyFromExistingSegmentProperty(METADATA_SEGMENT_PROPERTY_2));
        UPDATED_SEGMENT.setRestriction(new ConcreteRestriction(false, null, ComparisonType.EQUAL, null));
        segmentEntityMgr.createOrUpdate(UPDATED_SEGMENT);

        MetadataSegment retrieved = segmentEntityMgr.findByName(SEGMENT_NAME);
        assertNotNull(retrieved);
        assertEquals(retrieved.getDisplayName(), UPDATED_DISPLAY_SEGMENT_NAME);
        assertEquals(retrieved.getDescription(), UPDATED_SEGMENT_DESCRIPTION);
        assertEquals(retrieved.getDataCollection().getType(), DataCollectionType.Segmentation);
    }

    @Test(groups = "functional", dependsOnMethods = "updateSegment", enabled = false)
    public void deleteSegment() {
        MetadataSegment retrieved = segmentEntityMgr.findByName(SEGMENT_NAME);
        segmentEntityMgr.delete(retrieved);
        assertEquals(segmentEntityMgr.findAll().size(), 0);
    }

    private MetadataSegmentProperty copyFromExistingSegmentProperty(MetadataSegmentProperty existingProperty) {
        MetadataSegmentProperty metadataSegmentProperty = new MetadataSegmentProperty();

        metadataSegmentProperty.setOption(existingProperty.getOption());
        metadataSegmentProperty.setValue(existingProperty.getValue());

        return metadataSegmentProperty;
    }
}
