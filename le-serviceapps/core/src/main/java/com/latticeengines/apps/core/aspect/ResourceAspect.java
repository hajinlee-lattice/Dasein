package com.latticeengines.apps.core.aspect;

import javax.servlet.http.HttpServletRequest;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.InternalResourceBase;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Aspect
public class ResourceAspect {

    private InternalResourceBase internalResourceBase = new InternalResourceBase();

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Before("execution(* com.latticeengines.apps.*.controller.*.*(..)) && !execution(* com.latticeengines.apps.*.controller.HealthResource.*(..))")
    public void allControllerMethods(JoinPoint joinPoint) {
        checkHeader(joinPoint);
        String customerSpace = (String) joinPoint.getArgs()[0];
        setSecurityContext(customerSpace);
    }

    private void setSecurityContext(String customerSpace) {
        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace);
        if (tenant == null) {
            throw new RuntimeException(String.format("No tenant found with id %s", customerSpace));
        }
        MultiTenantContext.setTenant(tenant);
    }

    private void checkHeader(JoinPoint joinPoint) {
        Object[] args = joinPoint.getArgs();

        for (Object arg : args) {
            if (arg instanceof HttpServletRequest) {
                internalResourceBase.checkHeader((HttpServletRequest) arg);
            }
        }
    }
}
