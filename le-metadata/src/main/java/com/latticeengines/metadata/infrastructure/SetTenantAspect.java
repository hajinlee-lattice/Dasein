package com.latticeengines.metadata.infrastructure;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.TenantToken;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

@Aspect
public class SetTenantAspect {

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Before("execution(* com.latticeengines.metadata.service.impl.SegmentServiceImpl.*(..))")
    public void allMethodsSegmentService(JoinPoint joinPoint) {
        String customerSpace = (String) joinPoint.getArgs()[0];
        setSecurityContext(customerSpace.toString());
    }

    @Before("execution(* com.latticeengines.metadata.service.impl.MetadataServiceImpl.*(..))")
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

    @Before("execution(* com.latticeengines.metadata.service.impl.DataCollectionServiceImpl.*(..))")
    public void allMethodsDataCollectionService(JoinPoint joinPoint) {
        String customerSpace = (String) joinPoint.getArgs()[0];
        setSecurityContext(customerSpace);
    }

    @Before("execution(* com.latticeengines.metadata.service.impl.StatisticsContainerServiceImpl.*(..))")
    public void allMethodsStatisticsContainerService(JoinPoint joinPoint) {
        String customerSpace = (String) joinPoint.getArgs()[0];
        setSecurityContext(customerSpace);
    }

    @Before("execution(* com.latticeengines.metadata.service.impl.DataFeedServiceImpl.*(..))")
    public void allMethodsDataFeedService(JoinPoint joinPoint) {
        String customerSpace = (String) joinPoint.getArgs()[0];
        setSecurityContext(customerSpace);
    }

    @Before("execution(* com.latticeengines.metadata.service.impl.DataFeedTaskServiceImpl.*(..))")
    public void allMethodsDataFeedTaskService(JoinPoint joinPoint) {
        String customerSpace = (String) joinPoint.getArgs()[0];
        setSecurityContext(customerSpace);
    }

    private void setSecurityContext(String customerSpace) {
        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace.toString());
        if (tenant == null) {
            throw new RuntimeException(String.format("No tenant found with id %s", customerSpace.toString()));
        }
        setSecurityContext(tenant);
    }

    public void setSecurityContext(Tenant tenant) {
        SecurityContext securityCtx = SecurityContextHolder.createEmptyContext();
        securityCtx.setAuthentication(new TenantToken(tenant));
        SecurityContextHolder.setContext(securityCtx);
    }

}
