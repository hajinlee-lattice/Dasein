package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.metadata.entitymgr.SegmentEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.functionalframework.DataCollectionFunctionalTestNGBase;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class MetadataSegmentEntityMgrImplTestNG extends DataCollectionFunctionalTestNGBase {
    @Autowired
    private SegmentEntityMgr segmentEntityMgr;

    @Autowired
    private TableEntityMgr tableEntityMgr;

    private static final String SEGMENT_NAME = "SEGMENT_NAME";
    private static final String OTHER_SEGMENT_NAME = "OTHER_SEGMENT_NAME";

    private static final String SEGMENT_DISPLAY_NAME = "SEGMENT_DISPLAY_NAME";
    private static final String UPDATED_DISPLAY_SEGMENT_NAME = "UPDATED_DISPLAY_SEGMENT_NAME";
    private static final String SEGMENT_DESCRIPTION = "SEGMENT_DESCRIPTION";
    private static final String UPDATED_SEGMENT_DESCRIPTION = "UPDATED_SEGMENT_DESCRIPTION";
    private static final MetadataSegment METADATA_SEGMENT = new MetadataSegment();

    private Attribute arbitraryAttribute;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        createSegmentInOtherTenant();

        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace1));
        Table table = tableEntityMgr.findByName(TABLE1);
        addTableToCollection(table, TableRoleInCollection.BucketedAccount);

        arbitraryAttribute = getTablesInCollection().get(0).getAttributes().get(5);

        MetadataSegment master = segmentEntityMgr.findMasterSegment(collectionName);
        Assert.assertNotNull(master);
    }

    private void createSegmentInOtherTenant() {
        MetadataSegment otherSegment = new MetadataSegment();
        otherSegment.setName(OTHER_SEGMENT_NAME);
        otherSegment.setDisplayName("Other");
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace2));
        otherSegment.setDataCollection(dataCollection);
        segmentEntityMgr.createOrUpdate(otherSegment);
    }

    @Test(groups = "functional")
    public void createSegment() {
        METADATA_SEGMENT.setName(SEGMENT_NAME);
        METADATA_SEGMENT.setDisplayName(SEGMENT_DISPLAY_NAME);
        METADATA_SEGMENT.setDescription(SEGMENT_DESCRIPTION);
        METADATA_SEGMENT.setUpdated(new Date());
        METADATA_SEGMENT.setCreated(new Date());
        METADATA_SEGMENT.setRestriction(
                Restriction.builder().let(BusinessEntity.Account, arbitraryAttribute.getName()).eq(null).build());
        METADATA_SEGMENT.setDataCollection(dataCollection);
        segmentEntityMgr.createOrUpdate(METADATA_SEGMENT);

        MetadataSegment retrieved = segmentEntityMgr.findByName(SEGMENT_NAME);
        assertNotNull(retrieved);
        assertEquals(retrieved.getName(), METADATA_SEGMENT.getName());
        assertEquals(retrieved.getDisplayName(), METADATA_SEGMENT.getDisplayName());
        assertEquals(((ConcreteRestriction) retrieved.getRestriction()).getRelation(), ComparisonType.EQUAL);
        assertFalse(retrieved.getMasterSegment());
    }

    @Test(groups = "functional", dependsOnMethods = "createSegment")
    public void updateSegment() {
        MetadataSegment UPDATED_SEGMENT = new MetadataSegment();
        UPDATED_SEGMENT.setName(SEGMENT_NAME);
        UPDATED_SEGMENT.setDisplayName(UPDATED_DISPLAY_SEGMENT_NAME);
        UPDATED_SEGMENT.setDescription(UPDATED_SEGMENT_DESCRIPTION);
        UPDATED_SEGMENT.setUpdated(new Date());
        UPDATED_SEGMENT.setCreated(new Date());
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, "BUSINESS_NAME").eq("Hello")
                .build();
        UPDATED_SEGMENT.setRestriction(restriction);
        UPDATED_SEGMENT.setDataCollection(dataCollection);
        segmentEntityMgr.createOrUpdate(UPDATED_SEGMENT);

        MetadataSegment retrieved = segmentEntityMgr.findByName(SEGMENT_NAME);
        assertNotNull(retrieved);
        assertEquals(retrieved.getDisplayName(), UPDATED_DISPLAY_SEGMENT_NAME);
        assertEquals(retrieved.getDescription(), UPDATED_SEGMENT_DESCRIPTION);
        assertFalse(retrieved.getMasterSegment());

        Statistics statistics = new Statistics();
        StatisticsContainer container = new StatisticsContainer();
        container.setStatistics(statistics);
        container.setVersion(dataCollectionEntityMgr.getActiveVersion());
        segmentEntityMgr.upsertStats(SEGMENT_NAME, container);

        retrieved = segmentEntityMgr.findByName(SEGMENT_NAME);
        assertNotNull(retrieved);
    }

    @Test(groups = "functional", dependsOnMethods = "updateSegment")
    public void deleteSegment() {
        MetadataSegment retrieved = segmentEntityMgr.findByName(SEGMENT_NAME);
        segmentEntityMgr.delete(retrieved);
        assertEquals(segmentEntityMgr.findAll().size(), 0);
    }
}
