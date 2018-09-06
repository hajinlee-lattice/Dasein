package com.latticeengines.dante.testframework;

import javax.inject.Inject;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.testframework.exposed.service.GlobalAuthTestBed;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;


public abstract class DanteDeploymentTestNGBase extends DanteTestNGBase {

    @Inject
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
