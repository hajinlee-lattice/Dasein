package com.latticeengines.metadata.entitymgr.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatusDetail;
import com.latticeengines.domain.exposed.metadata.MigrationTrack;
import com.latticeengines.domain.exposed.metadata.MigrationTrackImportAction;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.security.TenantType;
import com.latticeengines.metadata.entitymgr.MigrationTrackEntityMgr;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;

public class MigrationTrackEntityMgrImplTestNG extends MetadataFunctionalTestNGBase {

    @Inject
    private MigrationTrackEntityMgr migrationTrackEntityMgr;

    private static final DataCollection.Version VERSION = DataCollection.Version.Blue;
    private static final String STATSNAME = "Test";
    private static Tenant tenant1, tenant2, untracked;
    private static Map<TableRoleInCollection, String[]> ACTIVETABLE = new HashMap<>();
    private static final MigrationTrackImportAction IMPORTACTION = new MigrationTrackImportAction();
    private static final DataCollectionStatusDetail DETAIL = new DataCollectionStatusDetail();
    private static final TableRoleInCollection ROLE = TableRoleInCollection.BucketedAccount;
    private static byte[] CUBESDATA = new byte[5];

    private static MigrationTrack track1 = new MigrationTrack();
    private static MigrationTrack track2 = new MigrationTrack();

    @BeforeClass(groups = "functional")
    private void getTestData() {
        super.setup();

        tenant1 = tenantEntityMgr.findByTenantId(customerSpace1);
        tenant2 = tenantEntityMgr.findByTenantId(customerSpace2);
        untracked = new Tenant();
        Assert.assertNotNull(tenant1);
        Assert.assertNotNull(tenant2);
        Assert.assertNotNull(untracked);

        String[] tableNames = {"This table", "That table"};
        ACTIVETABLE.put(ROLE, tableNames);

        IMPORTACTION.getActions().add(-1L);
        IMPORTACTION.getActions().add(-2L);

        track1.setStatus(MigrationTrack.Status.STARTED);
        track1.setVersion(VERSION);
        track1.setStatsName(STATSNAME);
        track1.setTenant(tenant1);
        track1.setCurActiveTable(ACTIVETABLE);
        track1.setImportAction(IMPORTACTION);
        track1.setCollectionStatusDetail((DETAIL));
        track1.setStatsCubesData(CUBESDATA);

        track2.setStatus(MigrationTrack.Status.COMPLETED);
        track2.setVersion(VERSION);
        track2.setStatsName(STATSNAME);
        track2.setTenant(tenant2);
        track2.setCurActiveTable(ACTIVETABLE);
        track2.setImportAction(IMPORTACTION);
        track2.setCollectionStatusDetail((DETAIL));
        track2.setStatsCubesData(CUBESDATA);

        untracked.setId("untracked");
        untracked.setName("untracked");
        untracked.setRegisteredTime(-2L);
        untracked.setStatus(TenantStatus.ACTIVE);
        untracked.setTenantType(TenantType.QA);
        untracked.setUiVersion("untracked");
    }

    @AfterClass(groups = "functional")
    private void removeTestData() {
        super.cleanup();
    }

    @Test(groups = "functional", dataProvider = "entityProvider")
    public void testCreate(Tenant tenant, MigrationTrack track) {
        Assert.assertNotNull(migrationTrackEntityMgr);
        Assert.assertNotNull(track);
        migrationTrackEntityMgr.create(track);
        Assert.assertNotNull(migrationTrackEntityMgr.findByKey(track));
        Assert.assertEquals(track.getPid(), migrationTrackEntityMgr.findByKey(track).getPid());
    }

    @Test(groups = "functional", dataProvider = "entityProvider", dependsOnMethods = {"testCreate"})
    public void testRead(Tenant tenant, MigrationTrack track) {
        MigrationTrack created = migrationTrackEntityMgr.findByKey(track);

        Assert.assertEquals(track.getStatus(), created.getStatus());
        Assert.assertEquals(VERSION, created.getVersion());
        Assert.assertNotNull(created.getCurActiveTable());
        Assert.assertNotNull(created.getCurActiveTable().get(ROLE));
        Assert.assertArrayEquals(ACTIVETABLE.get(ROLE), created.getCurActiveTable().get(ROLE));
        Assert.assertNotNull(created.getImportAction());
        Assert.assertArrayEquals(IMPORTACTION.getActions().toArray(), created.getImportAction().getActions().toArray());
        Assert.assertEquals(DETAIL.getEvaluationDate(), created.getCollectionStatusDetail().getEvaluationDate());
        Assert.assertArrayEquals(CUBESDATA, created.getStatsCubesData());
        Assert.assertEquals(STATSNAME, created.getStatsName());
        Assert.assertEquals(tenant.getPid(), created.getTenant().getPid());
    }

    @Test(groups = "functional", dependsOnMethods = {"testCreate"})
    public void testGetTenantsByStatus() {
        List<Long> found, actualStarted, actualCompleted;

        actualStarted = new ArrayList<>();
        actualStarted.add(tenant1.getPid());
        actualCompleted = new ArrayList<>();
        actualCompleted.add(tenant2.getPid());
        found = migrationTrackEntityMgr.getTenantPidsByStatus(MigrationTrack.Status.STARTED);
        Assert.assertNotNull(found);
        Assert.assertArrayEquals(actualStarted.toArray(), found.toArray());

        found = migrationTrackEntityMgr.getTenantPidsByStatus(MigrationTrack.Status.COMPLETED);
        Assert.assertNotNull(found);
        Assert.assertArrayEquals(actualCompleted.toArray(), found.toArray());
    }

    @Test(groups = "functional", dataProvider = "entityProvider", dependsOnMethods = {"testCreate"})
    public void testFindByTenant(Tenant tenant, MigrationTrack track) {
        MigrationTrack created = migrationTrackEntityMgr.findByTenant(tenant);

        Assert.assertEquals(track.getPid(), created.getPid());
    }

    @Test(groups = "functional", dataProvider = "entityProvider", dependsOnMethods = {"testCreate"})
    public void testTrackedTenantsConditions(Tenant tenant, MigrationTrack track) {
        track = migrationTrackEntityMgr.findByTenant(tenant);
        if (track.getStatus().equals(MigrationTrack.Status.STARTED)) {
            Assert.assertTrue(migrationTrackEntityMgr.tenantInMigration(tenant));
            Assert.assertFalse(migrationTrackEntityMgr.canDeleteOrRenameTable(tenant, "This table"));
        } else {
            Assert.assertFalse(migrationTrackEntityMgr.tenantInMigration(tenant));
            Assert.assertTrue(migrationTrackEntityMgr.canDeleteOrRenameTable(tenant, "This table"));
        }
        // Can delete tables not in curActiveTable regardless of tenant status
        Assert.assertTrue(migrationTrackEntityMgr.canDeleteOrRenameTable(tenant, "can delete this table"));
    }

    @Test(groups = "functional")
    public void testUntrackedTenantsConditions() {
        tenantEntityMgr.create(untracked);
        Assert.assertFalse(migrationTrackEntityMgr.tenantInMigration(untracked));
        Assert.assertTrue(migrationTrackEntityMgr.canDeleteOrRenameTable(untracked, "This table"));
        tenantEntityMgr.delete(untracked);
    }

    @DataProvider(name = "entityProvider")
    public Object[][] entityProvider() {
        return new Object[][]{{tenant1, track1}, {tenant2, track2}};
    }

}
