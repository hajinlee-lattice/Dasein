package com.latticeengines.metadata.infrastructure;

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

    @Before("execution(* com.latticeengines.metadata.service.impl.MetadataServiceImpl.*(..)) && " +
            "!@annotation(com.latticeengines.metadata.annotation.NoCustomSpaceAndType)")
    public void allMethodsMetadataService(JoinPoint joinPoint) {
        CustomerSpace customerSpace = (CustomerSpace) joinPoint.getArgs()[0];
        setSecurityContext(customerSpace.toString());
    }

    @Before("execution(* com.latticeengines.metadata.service.impl.ArtifactServiceImpl.*(..))")
    public void allMethodsArtifactService(JoinPoint joinPoint) {
        String customerSpace = (String) joinPoint.getArgs()[0];
        setSecurityContext(customerSpace);
    }

    @Before("execution(* com.latticeengines.metadata.service.impl.ModuleServiceImpl.*(..))")
    public void allMethodsModuleService(JoinPoint joinPoint) {
        String customerSpace = (String) joinPoint.getArgs()[0];
        setSecurityContext(customerSpace);
    }

    private void setSecurityContext(String customerSpace) {
        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace.toString());
        if (tenant == null) {
            throw new RuntimeException(String.format("No tenant found with id %s", customerSpace.toString()));
        }
        MultiTenantContext.setTenant(tenant);
    }

}
