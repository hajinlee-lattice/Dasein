package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.ImportMigrateTrackingService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.cdl.ImportMigrateTracking;

public class ImportImportMigrateTrackingServiceImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private ImportMigrateTrackingService importMigrateTrackingService;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
    }

    @Test(groups = "functional")
    public void testMigrateTrackingService() {
        ImportMigrateTracking importMigrateTracking = importMigrateTrackingService.create(mainCustomerSpace);
        Assert.assertNotNull(importMigrateTracking.getPid());
        Assert.assertEquals(importMigrateTracking.getStatus(), ImportMigrateTracking.Status.STARTING);
        importMigrateTrackingService.updateStatus(mainCustomerSpace, importMigrateTracking.getPid(), ImportMigrateTracking.Status.FAILED);
        importMigrateTracking = importMigrateTrackingService.getByPid(mainCustomerSpace, importMigrateTracking.getPid());
        Assert.assertEquals(importMigrateTracking.getStatus(), ImportMigrateTracking.Status.FAILED);
        importMigrateTracking = importMigrateTrackingService.create(mainCustomerSpace);
        Assert.assertNotNull(importMigrateTracking.getPid());
        Assert.expectThrows(RuntimeException.class, () -> importMigrateTrackingService.create(mainCustomerSpace));
        importMigrateTrackingService.updateStatus(mainCustomerSpace, importMigrateTracking.getPid(), ImportMigrateTracking.Status.MIGRATING);
        Assert.expectThrows(RuntimeException.class, () -> importMigrateTrackingService.create(mainCustomerSpace));
    }
}
