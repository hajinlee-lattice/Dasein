package com.latticeengines.apps.cdl.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.Date;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.cdl.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.SegmentEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.apps.cdl.util.ActionContext;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class MetadataSegmentEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(MetadataSegmentEntityMgrImplTestNG.class);

    @Inject
    private SegmentEntityMgr segmentEntityMgr;

    @Inject
    private TableEntityMgr tableEntityMgr;

    @Inject
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    private String segmentName;

    private static final String TABLE_NAME = "Account1";
    private static final String OTHER_SEGMENT_NAME = NamingUtils.uuid("OTHER_SEGMENT_NAME");

    private static final String SEGMENT_DISPLAY_NAME = "SEGMENT_DISPLAY_NAME";
    private static final String UPDATED_DISPLAY_SEGMENT_NAME = "UPDATED_DISPLAY_SEGMENT_NAME";
    private static final String SEGMENT_DESCRIPTION = "SEGMENT_DESCRIPTION";
    private static final String UPDATED_SEGMENT_DESCRIPTION = "UPDATED_SEGMENT_DESCRIPTION";
    private static final MetadataSegment METADATA_SEGMENT = new MetadataSegment();

    private static final Long ZERO = 0L;

    @BeforeClass(groups = "functional")
    public void setup() {
        segmentName = NamingUtils.uuid("SEGMENT_NAME");
        setupTestEnvironmentWithDataCollection();
        createSegmentInOtherTenant();

        createTable(TABLE_NAME);
        Table table = tableEntityMgr.findByName(TABLE_NAME);
        addTableToCollection(table, TableRoleInCollection.BucketedAccount);

        MetadataSegment master = segmentEntityMgr.findMasterSegment(collectionName);
        Assert.assertNotNull(master);
        ActionContext.remove();
    }

    @AfterClass(groups = "functional")
    public void cleanupActionContext() {
        ActionContext.remove();
    }

    private void createSegmentInOtherTenant() {
        String tenantId = TestFrameworkUtils.generateTenantName();
        testBed.addExtraTestTenant(tenantId);
        MetadataSegment otherSegment = new MetadataSegment();
        otherSegment.setName(OTHER_SEGMENT_NAME);
        otherSegment.setDisplayName("Other");
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(CustomerSpace.parse(tenantId).toString()));
        otherSegment.setTenant(MultiTenantContext.getTenant());
        otherSegment.setDataCollection(dataCollection);
        segmentEntityMgr.createSegment(otherSegment);
        MultiTenantContext.setTenant(mainTestTenant);
    }

    @Test(groups = "functional")
    public void createSegment() throws InterruptedException {
        Date preCreateTime = new Date();
        log.info("Start create test at " + preCreateTime.getTime());
        Thread.sleep(1000);

        METADATA_SEGMENT.setName(segmentName);
        METADATA_SEGMENT.setDisplayName(SEGMENT_DISPLAY_NAME);
        METADATA_SEGMENT.setDescription(SEGMENT_DESCRIPTION);
        METADATA_SEGMENT.setAccountRestriction(Restriction.builder().let(BusinessEntity.Account, "A").eq(null).build());
        METADATA_SEGMENT.setDataCollection(dataCollection);
        METADATA_SEGMENT.setTenant(MultiTenantContext.getTenant());
        segmentEntityMgr.createSegment(METADATA_SEGMENT);

        MetadataSegment retrieved = segmentEntityMgr.findByName(segmentName);
        assertNotNull(retrieved);
        assertNull(retrieved.getProducts());

        assertEquals(retrieved.getAccounts(), ZERO);
        assertEquals(retrieved.getContacts(), ZERO);
        assertEquals(retrieved.getName(), METADATA_SEGMENT.getName());
        assertEquals(retrieved.getDisplayName(), METADATA_SEGMENT.getDisplayName());
        assertEquals(((ConcreteRestriction) retrieved.getAccountRestriction()).getRelation(), ComparisonType.EQUAL);
        assertFalse(retrieved.getMasterSegment());

        log.info("Finish create test at " + new Date().getTime());

        assertNotNull(retrieved.getCreated());
        assertTrue(preCreateTime.before(retrieved.getCreated()));
        log.info("Created time is " + retrieved.getCreated().getTime());
        assertNotNull(retrieved.getUpdated());
        assertTrue(preCreateTime.before(retrieved.getUpdated()));
        Thread.sleep(1500);
    }

    @Test(groups = "functional", dependsOnMethods = "createSegment")
    public void updateSegment() throws InterruptedException {
        Action action = ActionContext.getAction();
        Assert.assertNull(action);
        Date preUpdateTime = new Date();
        log.info("Start create test at " + preUpdateTime.getTime());
        Thread.sleep(1500);

        MetadataSegment UPDATED_SEGMENT = new MetadataSegment();
        UPDATED_SEGMENT.setPid(METADATA_SEGMENT.getPid());
        UPDATED_SEGMENT.setName(segmentName);
        UPDATED_SEGMENT.setDisplayName(UPDATED_DISPLAY_SEGMENT_NAME);
        UPDATED_SEGMENT.setDescription(UPDATED_SEGMENT_DESCRIPTION);
        Restriction restriction = Restriction.builder().let(BusinessEntity.Account, "BUSINESS_NAME").eq("Hello")
                .build();
        UPDATED_SEGMENT.setAccountRestriction(restriction);
        UPDATED_SEGMENT.setDataCollection(dataCollection);

        UPDATED_SEGMENT.setAccounts(1L);
        UPDATED_SEGMENT.setContacts(2L);
        UPDATED_SEGMENT.setProducts(3L);

        segmentEntityMgr.updateSegment(UPDATED_SEGMENT, segmentEntityMgr.findByName(segmentName));

        MetadataSegment retrieved = segmentEntityMgr.findByName(segmentName);
        assertNotNull(retrieved);
        assertEquals(retrieved.getDisplayName(), UPDATED_DISPLAY_SEGMENT_NAME);
        assertEquals(retrieved.getDescription(), UPDATED_SEGMENT_DESCRIPTION);
        assertEquals(retrieved.getAccounts(), new Long(1));
        assertEquals(retrieved.getContacts(), new Long(2));
        assertEquals(retrieved.getProducts(), new Long(3));
        assertFalse(retrieved.getMasterSegment());

        StatisticsContainer container = new StatisticsContainer();
        container.setStatsCubes(ImmutableMap.of("Account", new StatsCube()));
        container.setVersion(dataCollectionEntityMgr.findActiveVersion());
        segmentEntityMgr.upsertStats(segmentName, container);

        retrieved = segmentEntityMgr.findByName(segmentName);
        assertNotNull(retrieved);

        log.info("Finish update test at " + new Date().getTime());

        assertTrue(preUpdateTime.after(retrieved.getCreated()),
                String.format("Created time %d should be before the pre-update time %d",
                        retrieved.getCreated().getTime(), preUpdateTime.getTime()));
        assertTrue(preUpdateTime.before(retrieved.getUpdated()),
                String.format("Updated time %d should be after the pre-update time %d",
                        retrieved.getUpdated().getTime(), preUpdateTime.getTime()));

        action = ActionContext.getAction();
        Assert.assertNotNull(action);
        Assert.assertEquals(action.getType(), ActionType.METADATA_SEGMENT_CHANGE);
    }

    @Test(groups = "functional", dependsOnMethods = "updateSegment")
    public void deleteSegment() {
        MetadataSegment retrieved = segmentEntityMgr.findByName(segmentName);
        segmentEntityMgr.delete(retrieved, false, false);
        assertEquals(segmentEntityMgr.getAllDeletedSegments().size(), 1);
        assertEquals(segmentEntityMgr.findAll().size(), 1);
        retrieved = segmentEntityMgr.findByName(segmentName);
        Assert.assertNotNull(retrieved);
        Assert.assertTrue(retrieved.getDeleted());

        segmentEntityMgr.delete(retrieved, false, true);
        assertEquals(segmentEntityMgr.findAll().size(), 1);
    }
}
