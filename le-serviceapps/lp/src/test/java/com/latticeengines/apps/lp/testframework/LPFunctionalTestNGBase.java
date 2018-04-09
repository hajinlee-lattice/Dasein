package com.latticeengines.apps.lp.testframework;

import javax.inject.Inject;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.testframework.service.impl.GlobalAuthFunctionalTestBed;

public abstract class LPFunctionalTestNGBase extends AbstractLPTestNGBase {

    @Inject
    protected GlobalAuthFunctionalTestBed testBed;

    protected Tenant mainTestTenant;
    protected String mainCustomerSpace;

    protected void setupTestEnvironment() {
        testBed.bootstrap(1);
        mainTestTenant = testBed.getMainTestTenant();
        mainCustomerSpace = mainTestTenant.getId();
        MultiTenantContext.setTenant(mainTestTenant);
        testBed.switchToSuperAdmin();
    }

}
