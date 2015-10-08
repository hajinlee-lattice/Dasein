package com.latticeengines.metadata.service.impl;

import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.TenantToken;

@Aspect
public class SetTenantAspect {

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Before("execution(* com.latticeengines.metadata.service.impl.MetadataServiceImpl.*(..))")
    public void allMethods(JoinPoint joinPoint) {
        CustomerSpace customerSpace = (CustomerSpace) joinPoint.getArgs()[0];
        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace.toString());
        if (tenant == null) {
            throw new RuntimeException("No tenant found with id " + tenant.getId());
        }
        setSecurityContext(tenant);
    }
    
    public void setSecurityContext(Tenant tenant) {
        SecurityContext securityCtx = SecurityContextHolder.createEmptyContext();
        securityCtx.setAuthentication(new TenantToken(tenant));
        SecurityContextHolder.setContext(securityCtx);
    }

}
