package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.MigrateTrackingService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.cdl.MigrateTracking;

public class MigrateTrackingServiceImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private MigrateTrackingService migrateTrackingService;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
    }

    @Test(groups = "functional")
    public void testMigrateTrackingService() {
        MigrateTracking migrateTracking = migrateTrackingService.create(mainCustomerSpace);
        Assert.assertNotNull(migrateTracking.getPid());
        Assert.assertEquals(migrateTracking.getStatus(), MigrateTracking.Status.STARTING);
        migrateTrackingService.updateStatus(mainCustomerSpace, migrateTracking.getPid(), MigrateTracking.Status.FAILED);
        migrateTracking = migrateTrackingService.getByPid(mainCustomerSpace, migrateTracking.getPid());
        Assert.assertEquals(migrateTracking.getStatus(), MigrateTracking.Status.FAILED);
        migrateTracking = migrateTrackingService.create(mainCustomerSpace);
        Assert.assertNotNull(migrateTracking.getPid());
        Assert.expectThrows(RuntimeException.class, () -> migrateTrackingService.create(mainCustomerSpace));
        migrateTrackingService.updateStatus(mainCustomerSpace, migrateTracking.getPid(), MigrateTracking.Status.MIGRATING);
        Assert.expectThrows(RuntimeException.class, () -> migrateTrackingService.create(mainCustomerSpace));
    }
}
