package com.latticeengines.apps.core.aspect;

import javax.inject.Inject;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;

@Aspect
public class ResourceAspect {

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Before("execution(* com.latticeengines.apps.*.controller.*.*(..)) " +
            "&& !@annotation(com.latticeengines.apps.core.annotation.NoCustomerSpace)")
    public void allControllerMethods(JoinPoint joinPoint) {
        String customerSpace = (String) joinPoint.getArgs()[0];
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        setMultiTenantContext(customerSpace);
    }

    private void setMultiTenantContext(String customerSpace) {
        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace);
        if (tenant == null) {
            throw new RuntimeException(String.format("No tenant found with id %s", customerSpace));
        }
        MultiTenantContext.setTenant(tenant);
    }
}
