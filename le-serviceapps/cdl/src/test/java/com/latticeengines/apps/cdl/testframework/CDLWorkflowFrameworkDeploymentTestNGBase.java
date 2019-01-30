package com.latticeengines.apps.cdl.testframework;

import static org.testng.Assert.assertNotNull;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;
import com.latticeengines.yarn.functionalframework.YarnFunctionalTestNGBase;

public abstract class CDLWorkflowFrameworkDeploymentTestNGBase extends CDLWorkflowFrameworkTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CDLWorkflowFrameworkDeploymentTestNGBase.class);

    @Resource(name = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed testBed;

    public void setup() throws Exception {
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
