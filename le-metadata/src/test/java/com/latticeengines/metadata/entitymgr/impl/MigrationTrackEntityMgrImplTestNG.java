package com.latticeengines.metadata.entitymgr.impl;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MigrationTrack;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.entitymgr.MigrationTrackEntityMgr;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:test-metadata-context.xml",
        "classpath:common-testclient-env-context.xml", "classpath:metadata-aspects-context.xml" })
public class MigrationTrackEntityMgrImplTestNG extends MetadataFunctionalTestNGBase {

    @Autowired
    protected MigrationTrackEntityMgr migrationTrackEntityMgr;

    private final MigrationTrack.Status STATUS = MigrationTrack.Status.SCHEDULED;
    private final DataCollection.Version VERSION = DataCollection.Version.Blue;
    private final String STATSNAME = "Test";
    private final TableRoleInCollection ROLE = TableRoleInCollection.AccountBatchSlim;
    private Map<TableRoleInCollection, String[]> ACTIVETABLE;

    @BeforeClass(groups = "functional")
    private void getTestData() {
        super.setup();
    }

    @AfterClass(groups = "functional")
    private void removeTestData() {
        super.cleanup();
    }

    @Test(groups = "functional", dataProvider = "entityProvider")
    public void testCreate(Tenant tenant, MigrationTrack track) {
        Assert.assertNotNull(migrationTrackEntityMgr.findByKey(track));
        Assert.assertEquals(migrationTrackEntityMgr.findByKey(track).getPid(), track.getPid());
    }

    @Test(groups = "functional", dataProvider = "entityProvider", dependsOnMethods = { "testCreate" })
    public void testStatus(Tenant tenant, MigrationTrack track) {
        Assert.assertEquals(track.getStatus(), migrationTrackEntityMgr.findByKey(track).getStatus());
        Assert.assertEquals(STATUS, migrationTrackEntityMgr.findByKey(track).getStatus());
    }

    @Test(groups = "functional", dataProvider = "entityProvider", dependsOnMethods = { "testCreate" })
    public void testVersion(Tenant tenant, MigrationTrack track) {
        Assert.assertEquals(track.getVersion(), migrationTrackEntityMgr.findByKey(track).getVersion());
        Assert.assertEquals(VERSION, migrationTrackEntityMgr.findByKey(track).getVersion());
    }

    @Test(groups = "functional", dataProvider = "entityProvider", dependsOnMethods = { "testCreate" })
    public void testActiveTable(Tenant tenant, MigrationTrack track) {
        Assert.assertNotNull(migrationTrackEntityMgr.findByKey(track).getCurActiveTable());
        Assert.assertNotNull(migrationTrackEntityMgr.findByKey(track).getCurActiveTable().get(ROLE));
        Assert.assertArrayEquals(ACTIVETABLE.get(ROLE),
                migrationTrackEntityMgr.findByKey(track).getCurActiveTable().get(ROLE));
    }

    @Test(groups = "functional", dataProvider = "entityProvider", dependsOnMethods = { "testCreate" })
    public void testImportAction(Tenant tenant, MigrationTrack track) {
        Assert.assertEquals(track.getImportAction(), migrationTrackEntityMgr.findByKey(track).getImportAction());
    }

    @Test(groups = "functional", dataProvider = "entityProvider", dependsOnMethods = { "testCreate" })
    public void testStatusDetail(Tenant tenant, MigrationTrack track) {
        Assert.assertNull(track.getCollectionStatusDetail());
        Assert.assertNull(migrationTrackEntityMgr.findByKey(track).getCollectionStatusDetail());
    }

    @Test(groups = "functional", dataProvider = "entityProvider", dependsOnMethods = { "testCreate" })
    public void testStatsCubesData(Tenant tenant, MigrationTrack track) {
        Assert.assertArrayEquals(track.getStatsCubesData(),
                migrationTrackEntityMgr.findByKey(track).getStatsCubesData());
    }

    @Test(groups = "functional", dataProvider = "entityProvider", dependsOnMethods = { "testCreate" })
    public void testStatsName(Tenant tenant, MigrationTrack track) {
        Assert.assertEquals(track.getStatsName(), migrationTrackEntityMgr.findByKey(track).getStatsName());
        Assert.assertEquals(STATSNAME, migrationTrackEntityMgr.findByKey(track).getStatsName());
    }

    @Test(groups = "functional", dataProvider = "entityProvider", dependsOnMethods = { "testCreate" })
    public void testDataCollection(Tenant tenant, MigrationTrack track) {
        Assert.assertEquals(track.getDataCollection(), migrationTrackEntityMgr.findByKey(track).getDataCollection());
    }

    @Test(groups = "functional", dataProvider = "entityProvider", dependsOnMethods = { "testCreate" })
    public void testTenant(Tenant tenant, MigrationTrack track) {
        Assert.assertEquals(tenant.getPid(), migrationTrackEntityMgr.findByKey(track).getTenant().getPid());
    }

    @Test(groups = "functional", dataProvider = "entityProvider", dependsOnMethods = { "testCreate" })
    public void testFindByTenant(Tenant tenant, MigrationTrack track) {
        Assert.assertEquals(tenant.getPid(), migrationTrackEntityMgr.findByTenant(tenant).getTenant().getPid());
    }

    @DataProvider(name = "entityProvider")
    public Object[][] enittyProvider() {
        Tenant tenant1 = tenantEntityMgr.findByTenantId(customerSpace1),
                tenant2 = tenantEntityMgr.findByTenantId(customerSpace2);
        MigrationTrack track1, track2;

        Assert.assertNotNull(tenant1);
        Assert.assertNotNull(tenant2);

        if (ACTIVETABLE == null) {
            ACTIVETABLE = new HashMap<>();
            String[] tableNames = { "This table", "That table" };
            ACTIVETABLE.put(ROLE, tableNames);
        }

        if (migrationTrackEntityMgr.findByField("FK_TENANT_ID", tenant1.getPid()) == null) {
            track1 = new MigrationTrack();
            track1.setStatus(STATUS);
            track1.setVersion(VERSION);
            track1.setStatsName(STATSNAME);
            track1.setTenant(tenant1);
            track1.setCurActiveTable(ACTIVETABLE);
            track1.setCollectionStatusDetail(null);
            track1.setStatsCubesData(new byte[1]);
            Assert.assertNotNull(track1);
            migrationTrackEntityMgr.create(track1);
        } else {
            track1 = migrationTrackEntityMgr.findByField("FK_TENANT_ID", tenant1.getPid());
        }

        if (migrationTrackEntityMgr.findByField("FK_TENANT_ID", tenant2.getPid()) == null) {
            track2 = new MigrationTrack();
            track2.setStatus(STATUS);
            track2.setVersion(VERSION);
            track2.setStatsName(STATSNAME);
            track2.setTenant(tenant2);
            track2.setCurActiveTable(ACTIVETABLE);
            track2.setCollectionStatusDetail((null));
            track2.setStatsCubesData(new byte[5]);
            Assert.assertNotNull(track2);
            migrationTrackEntityMgr.create(track2);
        } else {
            track2 = migrationTrackEntityMgr.findByField("FK_TENANT_ID", tenant2.getPid());
        }

        return new Object[][] { { tenant1, track1 }, { tenant2, track2 } };
    }

}
