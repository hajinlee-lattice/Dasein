package com.latticeengines.apps.lp.testframework;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;

public abstract class LPDeploymentTestNGBase extends AbstractLPTestNGBase {

    @Autowired
    @Qualifier(value = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed deploymentTestBed;

    protected void setupSecurityContext(Tenant t) {
        MultiTenantContext.setTenant(t);
    }

}
