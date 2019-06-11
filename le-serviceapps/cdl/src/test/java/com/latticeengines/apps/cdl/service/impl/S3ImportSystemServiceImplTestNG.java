package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.S3ImportSystemService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;

public class S3ImportSystemServiceImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private S3ImportSystemService s3ImportSystemService;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
    }

    @Test(groups = "functional")
    public void testS3ImportSystem() {
        S3ImportSystem system1= new S3ImportSystem();
        system1.setTenant(mainTestTenant);
        system1.setName("SYSTEM1");
        system1.setDisplayName("SYSTEM1");
        system1.setSystemType(S3ImportSystem.SystemType.Salesforce);
        s3ImportSystemService.createS3ImportSystem(mainCustomerSpace, system1);

        S3ImportSystem system2= new S3ImportSystem();
        system2.setTenant(mainTestTenant);
        system2.setName("SYSTEM2");
        system2.setDisplayName("SYSTEM2");
        system2.setSystemType(S3ImportSystem.SystemType.Other);
        s3ImportSystemService.createS3ImportSystem(mainCustomerSpace, system2);

        S3ImportSystem system = s3ImportSystemService.getS3ImportSystem(mainCustomerSpace, "SYSTEM1");
        Assert.assertNotNull(system);
        Assert.assertEquals(system.getSystemType(), S3ImportSystem.SystemType.Salesforce);
        Assert.assertEquals(system.getPriority(), 1);
        system.setPriority(2);

        s3ImportSystemService.updateS3ImportSystem(mainCustomerSpace, system);

        List<S3ImportSystem> allSystems = s3ImportSystemService.getAllS3ImportSystem(mainCustomerSpace);

        Assert.assertEquals(allSystems.size(), 2);
        for (S3ImportSystem importSystem : allSystems) {
            if (importSystem.getName().equals("SYSTEM1")) {
                Assert.assertEquals(importSystem.getPriority(), 2);
            } else {
                Assert.assertEquals(importSystem.getPriority(), 1);
            }
        }

        S3ImportSystem system3 = new S3ImportSystem();
        system3.setTenant(mainTestTenant);
        system3.setName("SYSTEM2");
        system3.setSystemType(S3ImportSystem.SystemType.Eloqua);
        Assert.expectThrows(RuntimeException.class,
                () -> s3ImportSystemService.createS3ImportSystem(mainCustomerSpace, system3));
    }
}
