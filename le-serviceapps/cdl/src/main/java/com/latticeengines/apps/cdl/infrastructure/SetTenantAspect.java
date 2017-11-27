package com.latticeengines.apps.cdl.infrastructure;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.TenantToken;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

@Aspect
public class SetTenantAspect {
    private static final Logger log = LoggerFactory.getLogger(SetTenantAspect.class);

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Before("execution(* com.latticeengines.apps.cdl.service.impl.CDLJobServiceImpl.create*(..)) ")
    public void allCreateDataFeedJobService(JoinPoint joinPoint) {
        String customerSpace = (String) joinPoint.getArgs()[0];
        setSecurityContext(CustomerSpace.parse(customerSpace).toString());
    }

    @Before("execution(* com.latticeengines.apps.cdl.service.impl.CDLExternalSystemServiceImpl.*(..))")
    public void allCDLExternalSystemService(JoinPoint joinPoint) {
        String customerSpace = (String) joinPoint.getArgs()[0];
        setSecurityContext(CustomerSpace.parse(customerSpace).toString());
    }

    private void setSecurityContext(String customerSpace) {
        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace);
        if (tenant == null) {
            throw new RuntimeException(String.format("No tenant found with id %s", customerSpace));
        }
        setSecurityContext(tenant);
    }

    public void setSecurityContext(Tenant tenant) {
        SecurityContext securityCtx = SecurityContextHolder.createEmptyContext();
        securityCtx.setAuthentication(new TenantToken(tenant));
        SecurityContextHolder.setContext(securityCtx);
    }
}
