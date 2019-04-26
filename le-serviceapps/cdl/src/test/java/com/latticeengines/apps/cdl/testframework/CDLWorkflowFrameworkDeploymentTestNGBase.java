package com.latticeengines.apps.cdl.testframework;

import static org.testng.Assert.assertNotNull;

import javax.annotation.Resource;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;
import com.latticeengines.yarn.functionalframework.YarnFunctionalTestNGBase;

public abstract class CDLWorkflowFrameworkDeploymentTestNGBase extends CDLWorkflowFrameworkTestNGBase {

    @Resource(name = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed testBed;

    protected void setupTestEnvironment() {
        setupTestEnvironment(LatticeProduct.CG);
    }

    protected void setupTestEnvironment(LatticeProduct product) {
        testBed.bootstrapForProduct(product);
        mainTestTenant = testBed.getMainTestTenant();
        mainTestCustomerSpace = CustomerSpace.parse(mainTestTenant.getId());
        MultiTenantContext.setTenant(mainTestTenant);
        testBed.switchToSuperAdmin();
        assertNotNull(MultiTenantContext.getTenant());
        checkpointService.setMainTestTenant(mainTestTenant);
        setupYarnPlatform();
    }

    protected void setupYarnPlatform() {
        platformTestBase = new YarnFunctionalTestNGBase(yarnConfiguration);
        platformTestBase.setYarnClient(defaultYarnClient);
    }

}
