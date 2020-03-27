package com.latticeengines.apps.dcp.infrastructure;

import javax.inject.Inject;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;

@Aspect
public class SetTenantAspect {

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Before("execution(* com.latticeengines.apps.dcp.service.impl.ProjectServiceImpl.*(..))")
    public void allDCPProjectService(JoinPoint joinPoint) {
        String customerSpace = (String) joinPoint.getArgs()[0];
        setMultiTenantContext(CustomerSpace.parse(customerSpace).toString());
    }

    @Before("execution(* com.latticeengines.apps.dcp.service.impl.SourceServiceImpl.*(..))")
    public void allSourceService(JoinPoint joinPoint) {
        String customerSpace = (String) joinPoint.getArgs()[0];
        setMultiTenantContext(CustomerSpace.parse(customerSpace).toString());
    }

    @Before("execution(* com.latticeengines.apps.dcp.service.impl.UploadServiceImpl.*(..))")
    public void allUploadService(JoinPoint joinPoint) {
        String customerSpace = (String) joinPoint.getArgs()[0];
        setMultiTenantContext(CustomerSpace.parse(customerSpace).toString());
    }

    private void setMultiTenantContext(String customerSpace) {
        Tenant tenant = tenantEntityMgr.findByTenantId(CustomerSpace.parse(customerSpace).toString());
        if (tenant == null) {
            throw new RuntimeException(String.format("No tenant found with id %s", customerSpace));
        }
        MultiTenantContext.setTenant(tenant);
    }
}
