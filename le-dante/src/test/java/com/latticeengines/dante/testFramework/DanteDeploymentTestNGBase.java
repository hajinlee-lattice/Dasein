package com.latticeengines.dante.testFramework;

import com.latticeengines.testframework.exposed.service.GlobalAuthTestBed;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;

public abstract class DanteDeploymentTestNGBase extends DanteTestNGBase {

    @Autowired
    protected GlobalAuthDeploymentTestBed deploymentTestBed;

    @Override
    protected void setupRunEnvironment() {
        getTestBed().bootstrapForProduct(LatticeProduct.CG);
        mainTestTenant = getTestBed().getMainTestTenant();
        mainTestCustomerSpace = CustomerSpace.parse(mainTestTenant.getId());
        MultiTenantContext.setTenant(mainTestTenant);
    }

    protected GlobalAuthTestBed getTestBed() {
        return deploymentTestBed;
    }

}
