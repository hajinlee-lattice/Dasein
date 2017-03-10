package com.latticeengines.pls.end2end;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class CDLEndToEndDeploymentTestNG extends PlsDeploymentTestNGBase {

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
    }

    @Test(groups = "deployment", enabled = false)
    public void createSegment() {

    }

    @Test(groups = "deployment", dependsOnMethods = "createSegment", enabled = false)
    public void getNumAccountsForSegment() {

    }

    @Test(groups = "deployment", dependsOnMethods = "getNumAccountsForSegment", enabled = false)
    public void viewAccountsForSegment() {

    }

    @Test(groups = "deployment", dependsOnMethods = "viewAccountsForSegment", enabled = false)
    public void modifySegment() {
    }

    @Test(groups = "deployment", dependsOnMethods = "modifySegment", enabled = false)
    public void validateUpdatedStatistics() {

    }
}
