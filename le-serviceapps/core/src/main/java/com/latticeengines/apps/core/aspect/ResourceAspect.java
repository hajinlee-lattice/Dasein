package com.latticeengines.apps.core.aspect;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.DBConnectionContext;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;

@Aspect
public class ResourceAspect {

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Before("execution(* com.latticeengines.apps.*.controller.*.*(..)) "
            + "&& !@annotation(com.latticeengines.apps.core.annotation.NoCustomerSpace)")
    public void allControllerMethods(JoinPoint joinPoint) {
        if (joinPoint.getArgs().length > 0 && joinPoint.getArgs()[0] instanceof HttpServletRequest) {
            // for certain microservice endpoints (PlaymakerTenantResource -
            // authToken to *) we do not have mandatory customer space in URL
            // path. Skip setting multi tenant context for such calls
            return;
        }
        String customerSpace = (String) joinPoint.getArgs()[0];
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        setMultiTenantContext(customerSpace);
    }

    @Around("execution(* com.latticeengines.apps.*.controller.*.*(..)) " +
            "&& @annotation(com.latticeengines.common.exposed.annotation.UseReaderConnection)")
    public Object setReaderConnection(ProceedingJoinPoint joinPoint) throws Throwable {
        DBConnectionContext.setReaderConnection(Boolean.TRUE);
        Object obj = joinPoint.proceed();
        DBConnectionContext.setReaderConnection(Boolean.FALSE);
        return obj;
    }

    private void setMultiTenantContext(String customerSpace) {
        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace);
        if (tenant == null) {
            throw new RuntimeException(String.format("No tenant found with id %s", customerSpace));
        }
        MultiTenantContext.setTenant(tenant);
    }
}
