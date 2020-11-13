package com.latticeengines.apps.cdl.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
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
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.ListSegment;
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
    private String listSegmentName;

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
        listSegmentName = NamingUtils.uuid("LIST_SEGMENT_NAME");
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
        Thread.sleep(1500L);

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

        segmentEntityMgr.updateSegment(UPDATED_SEGMENT, segmentEntityMgr.findByName(segmentName));

        MetadataSegment retrieved = segmentEntityMgr.findByName(segmentName);
        assertNotNull(retrieved);
        assertEquals(retrieved.getDisplayName(), UPDATED_DISPLAY_SEGMENT_NAME);
        assertEquals(retrieved.getDescription(), UPDATED_SEGMENT_DESCRIPTION);
        assertEquals(retrieved.getAccounts(), Long.valueOf(1));
        assertEquals(retrieved.getContacts(), Long.valueOf(2));
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

        Date updated = retrieved.getUpdated();
        retrieved.setAccounts(2L);
        segmentEntityMgr.updateSegmentWithoutActionAndAuditing(retrieved, segmentEntityMgr.findByName(segmentName));

        RetryTemplate retry = RetryUtils.getRetryTemplate(5, Collections.singleton(AssertionError.class), null);
        retrieved = retry.execute(ctx -> {
            MetadataSegment segment2 = segmentEntityMgr.findByName(segmentName);
            Assert.assertEquals(segment2.getAccounts(), Long.valueOf(2));
            return segment2;
        });
        Assert.assertEquals(retrieved.getUpdated(), updated);
    }

    @Test(groups = "functional", dependsOnMethods = {"updateSegment", "createOrUpdateListSegment"})
    public void deleteSegment() {
        MetadataSegment retrieved = segmentEntityMgr.findByName(segmentName);
        segmentEntityMgr.delete(retrieved, false, false);
        assertEquals(segmentEntityMgr.getAllDeletedSegments().size(), 1);
        assertEquals(segmentEntityMgr.findAll().size(), 2);
        retrieved = segmentEntityMgr.findByName(segmentName);
        assertNotNull(retrieved);
        assertTrue(retrieved.getDeleted());
        segmentEntityMgr.delete(retrieved, false, true);
        assertEquals(segmentEntityMgr.findAll().size(), 2);
        verifyDelete(false, 1);
        verifyDelete(true, 1);
    }

    private void verifyDelete(boolean hardDelete, int totalSize) {
        MetadataSegment retrieved = segmentEntityMgr.findByName(listSegmentName, hardDelete);
        segmentEntityMgr.delete(retrieved, false, hardDelete);
        assertEquals(segmentEntityMgr.findAll().size(), totalSize);
        retrieved = segmentEntityMgr.findByName(listSegmentName);
        if (hardDelete) {
            assertNull(retrieved);
        } else {
            assertTrue(retrieved.getDeleted());
        }
    }

    @Test(groups = "functional")
    public void createOrUpdateListSegment() {
        MetadataSegment metadataSegment = new MetadataSegment();
        String segmentDisplayName = "list-segment-display-name";
        String segmentDescription = "list-segment-description";
        String externalSystem = "dataVision";
        String externalSegmentId = "dataVisionSegment";
        String s3DropFolder = "/latticeengines-qa-data-stage/datavision_segment/" + listSegmentName + "/input";
        ListSegment listSegment = createListSegment(externalSystem, externalSegmentId, s3DropFolder);
        metadataSegment.setName(listSegmentName);
        metadataSegment.setDisplayName(segmentDisplayName);
        metadataSegment.setDescription(segmentDescription);
        metadataSegment.setDataCollection(dataCollection);
        metadataSegment.setType(MetadataSegment.SegmentType.List);
        metadataSegment.setTenant(MultiTenantContext.getTenant());
        metadataSegment.setListSegment(listSegment);
        segmentEntityMgr.createListSegment(metadataSegment);

        metadataSegment = segmentEntityMgr.findByName(listSegmentName, true);
        verifyListSegment(metadataSegment, segmentDisplayName, segmentDescription, externalSystem, externalSegmentId);
        segmentDisplayName = "list-segment-display-name2";
        segmentDescription = "list-segment-description2";
        metadataSegment.setDisplayName(segmentDisplayName);
        metadataSegment.setDescription(segmentDescription);
        MetadataSegment existingSegment = segmentEntityMgr.findByName(metadataSegment.getName());
        segmentEntityMgr.updateListSegment(metadataSegment, existingSegment);
        metadataSegment = segmentEntityMgr.findByName(listSegmentName, true);
        verifyListSegment(metadataSegment, segmentDisplayName, segmentDescription, externalSystem, externalSegmentId);
        metadataSegment = segmentEntityMgr.findByExternalInfo(metadataSegment);
        verifyListSegment(metadataSegment, segmentDisplayName, segmentDescription, externalSystem, externalSegmentId);
        metadataSegment = segmentEntityMgr.findByExternalInfo(metadataSegment.getListSegment().getExternalSystem(),
                metadataSegment.getListSegment().getExternalSegmentId());
        verifyListSegment(metadataSegment, segmentDisplayName, segmentDescription, externalSystem, externalSegmentId);
        List<MetadataSegment> lists = segmentEntityMgr.findByType(MetadataSegment.SegmentType.List);
        assertEquals(lists.size(), 1);
    }

    private void verifyListSegment(MetadataSegment segment, String displayName, String description,
                                   String externalSystemName, String externalSegmentId) {
        assertNotNull(segment);
        assertEquals(segment.getDescription(), description);
        assertEquals(segment.getDisplayName(), displayName);
        assertEquals(segment.getName(), listSegmentName);
        ListSegment listSegment = segment.getListSegment();
        assertNotNull(listSegment);
        assertEquals(listSegment.getExternalSystem(), externalSystemName);
        assertEquals(listSegment.getExternalSegmentId(), externalSegmentId);
        assertNotNull(listSegment.getS3DropFolder());
    }

    private ListSegment createListSegment(String externalSystem, String externalSegmentId, String s3DropFolder) {
        ListSegment listSegment = new ListSegment();
        listSegment.setExternalSystem(externalSystem);
        listSegment.setExternalSegmentId(externalSegmentId);
        listSegment.setS3DropFolder(s3DropFolder);
        return listSegment;
    }
}
