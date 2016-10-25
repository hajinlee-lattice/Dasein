package com.latticeengines.pls.provisioning;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.security.exposed.service.TenantService;

public class CGProvisioningDeploymentTestNG extends PlsDeploymentTestNGBase {
    @Autowired
    private TenantService tenantService;

    @Test(groups = "deployment")
    public void testProvision() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
    }
}
