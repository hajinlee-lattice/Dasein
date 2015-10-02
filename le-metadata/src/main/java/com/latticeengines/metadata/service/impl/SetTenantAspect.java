package com.latticeengines.metadata.service.impl;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.TenantToken;

@Aspect
public class SetTenantAspect {

    @Before("execution(* com.latticeengines.metadata.service.impl.MetadataServiceImpl.get*(..))")
    public void getTable(JoinPoint joinPoint) {
        CustomerSpace customerSpace = (CustomerSpace) joinPoint.getArgs()[0];
        setSecurityContext(customerSpace);
    }
    
    public void setSecurityContext(CustomerSpace customerSpace) {
        SecurityContext securityCtx = SecurityContextHolder.createEmptyContext();
        Tenant tenant = new Tenant();
        tenant.setId(customerSpace.toString());
        tenant.setName(customerSpace.toString());
        securityCtx.setAuthentication(new TenantToken(tenant));
        SecurityContextHolder.setContext(securityCtx);
    }

}
