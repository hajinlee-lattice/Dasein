package com.latticeengines.pls.provisioning;

import javax.inject.Inject;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.security.exposed.service.TenantService;
public class CGProvisioningDeploymentTestNG extends PlsDeploymentTestNGBase {
    @Inject
    private TenantService tenantService;

    @Test(groups = "deployment")
    public void testProvision() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
    }
}
