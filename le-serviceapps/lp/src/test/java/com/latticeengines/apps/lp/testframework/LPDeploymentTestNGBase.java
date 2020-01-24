package com.latticeengines.apps.lp.testframework;

import javax.annotation.Resource;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;

public abstract class LPDeploymentTestNGBase extends AbstractLPTestNGBase {

    @Resource(name = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed deploymentTestBed;

    protected void setupSecurityContext(Tenant t) {
        MultiTenantContext.setTenant(t);
    }

}
