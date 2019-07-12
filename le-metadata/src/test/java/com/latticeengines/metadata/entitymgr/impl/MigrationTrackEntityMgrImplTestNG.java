package com.latticeengines.metadata.entitymgr.impl;

import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MigrationTrack;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;

public class MigrationTrackEntityMgrImplTestNG extends MetadataFunctionalTestNGBase {

    private MigrationTrack m;

    private Tenant tenant;

    @BeforeClass(groups = "functional")
    private void getTestData() {
        super.setup();
        tenant = tenantEntityMgr.findByTenantId(customerSpace1);
        Assert.assertNotNull(tenant);
        m = new MigrationTrack();
        m.setStatus(MigrationTrack.Status.SCHEDULED);
        m.setVersion(DataCollection.Version.Blue);
        m.setStatsName("Test");
        m.setTenant(tenant);
        migrationTrackEntityMgr.create(m);
    }

    @AfterClass(groups = "functional")
    private void removeTestData() {
        super.cleanup();
    }

    @Test(groups = "functional")
    public void testTenantMatch() {
        Assert.assertNotNull(tenant);
        Assert.assertNotNull(migrationTrackEntityMgr.findByTenant(tenant));
        Assert.assertEquals(tenant.getPid(), migrationTrackEntityMgr.findByTenant(tenant).getTenant().getPid());
    }

}
