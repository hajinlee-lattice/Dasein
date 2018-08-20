package com.latticeengines.apps.cdl.controller;

import org.testng.annotations.BeforeClass;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.security.Tenant;

public class MultiTenantRatingEngineResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        testBed.bootstrapForProduct(LatticeProduct.CG);
        Tenant tenant2 = testBed.getMainTestTenant();
        setupTestEnvironment();
        Tenant tenant1 = testBed.getMainTestTenant();
    }

}
