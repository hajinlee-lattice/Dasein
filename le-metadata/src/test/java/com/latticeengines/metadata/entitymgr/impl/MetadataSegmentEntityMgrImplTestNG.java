package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

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
    public void createSegment() throws InterruptedException {
        Date preCreateTime = new Date();
        System.out.println("Start create test at " + preCreateTime.getTime());
        Thread.sleep(1000);

        METADATA_SEGMENT.setName(SEGMENT_NAME);
        METADATA_SEGMENT.setDisplayName(SEGMENT_DISPLAY_NAME);
        METADATA_SEGMENT.setDescription(SEGMENT_DESCRIPTION);
        METADATA_SEGMENT.setAccountRestriction(
                Restriction.builder().let(BusinessEntity.Account, arbitraryAttribute.getName()).eq(null).build());
        METADATA_SEGMENT.setDataCollection(dataCollection);
        segmentEntityMgr.createOrUpdate(METADATA_SEGMENT);

        MetadataSegment retrieved = segmentEntityMgr.findByName(SEGMENT_NAME);
        assertNotNull(retrieved);

        assertNull(retrieved.getAccounts());
        assertNull(retrieved.getContacts());
        assertNull(retrieved.getProducts());

        assertEquals(retrieved.getName(), METADATA_SEGMENT.getName());
        assertEquals(retrieved.getDisplayName(), METADATA_SEGMENT.getDisplayName());
        assertEquals(((ConcreteRestriction) retrieved.getAccountRestriction()).getRelation(), ComparisonType.EQUAL);
        assertFalse(retrieved.getMasterSegment());

        System.out.println("Finish create test at " + new Date().getTime());

        assertNotNull(retrieved.getCreated());
        assertTrue(preCreateTime.before(retrieved.getCreated()));
        assertNotNull(retrieved.getUpdated());
        assertTrue(preCreateTime.before(retrieved.getUpdated()));
    }

    @Test(groups = "functional", dependsOnMethods = "createSegment")
    public void updateSegment() throws InterruptedException {
        Date preUpdateTime = new Date();
        System.out.println("Start create test at " + preUpdateTime.getTime());
        Thread.sleep(1000);

        MetadataSegment UPDATED_SEGMENT = new MetadataSegment();
        UPDATED_SEGMENT.setPid(METADATA_SEGMENT.getPid());
        UPDATED_SEGMENT.setName(SEGMENT_NAME);
        UPDATED_SEGMENT.setDisplayName(UPDATED_DISPLAY_SEGMENT_NAME);
        UPDATED_SEGMENT.setDescription(UPDATED_SEGMENT_DESCRIPTION);
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, "BUSINESS_NAME").eq("Hello")
                .build();
        UPDATED_SEGMENT.setAccountRestriction(restriction);
        UPDATED_SEGMENT.setDataCollection(dataCollection);

        UPDATED_SEGMENT.setAccounts(1L);
        UPDATED_SEGMENT.setContacts(2L);
        UPDATED_SEGMENT.setProducts(3L);

        segmentEntityMgr.createOrUpdate(UPDATED_SEGMENT);

        MetadataSegment retrieved = segmentEntityMgr.findByName(SEGMENT_NAME);
        assertNotNull(retrieved);
        assertEquals(retrieved.getDisplayName(), UPDATED_DISPLAY_SEGMENT_NAME);
        assertEquals(retrieved.getDescription(), UPDATED_SEGMENT_DESCRIPTION);
        assertEquals(retrieved.getAccounts(), new Long(1));
        assertEquals(retrieved.getContacts(), new Long(2));
        assertEquals(retrieved.getProducts(), new Long(3));
        assertFalse(retrieved.getMasterSegment());

        Statistics statistics = new Statistics();
        StatisticsContainer container = new StatisticsContainer();
        container.setStatistics(statistics);
        container.setVersion(dataCollectionEntityMgr.getActiveVersion());
        segmentEntityMgr.upsertStats(SEGMENT_NAME, container);

        retrieved = segmentEntityMgr.findByName(SEGMENT_NAME);
        assertNotNull(retrieved);

        System.out.println("Finish update test at " + new Date().getTime());

        assertTrue(preUpdateTime.after(retrieved.getCreated()),
                String.format("Created time %d should be before the pre-update time %d", retrieved.getCreated().getTime(),
                        preUpdateTime.getTime()));
        assertTrue(preUpdateTime.before(retrieved.getUpdated()),
                String.format("Updated time %d should be after the pre-update time %d", retrieved.getUpdated().getTime(),
                        preUpdateTime.getTime()));
    }

    @Test(groups = "functional", dependsOnMethods = "updateSegment")
    public void deleteSegment() {
        MetadataSegment retrieved = segmentEntityMgr.findByName(SEGMENT_NAME);
        segmentEntityMgr.delete(retrieved);
        assertEquals(segmentEntityMgr.findAll().size(), 0);
    }
}
