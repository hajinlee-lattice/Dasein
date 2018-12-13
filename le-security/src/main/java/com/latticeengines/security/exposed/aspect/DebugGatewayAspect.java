package com.latticeengines.security.exposed.aspect;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.ThreadContext;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

import com.latticeengines.camille.exposed.watchers.DebugGatewayWatcher;
import com.latticeengines.db.exposed.util.MultiTenantContext;

@Aspect
public class DebugGatewayAspect {

    @Around("@annotation(org.springframework.web.bind.annotation.RequestMapping) " //
            + "|| @annotation(org.springframework.web.bind.annotation.GetMapping)" //
            + "|| @annotation(org.springframework.web.bind.annotation.PostMapping)" //
            + "|| @annotation(org.springframework.web.bind.annotation.PutMapping)" //
            + "|| @annotation(org.springframework.web.bind.annotation.DeleteMapping)")
    public Object interceptDebugGateway(ProceedingJoinPoint joinPoint) throws Throwable {
        String tenantId = MultiTenantContext.getShortTenantId();
        if (StringUtils.isNotBlank(tenantId) && DebugGatewayWatcher.hasPassport(tenantId)) {
            ThreadContext.put("DebugGateway", "ON");
            try {
                return joinPoint.proceed();
            } finally {
                ThreadContext.remove("DebugGateway");
            }
        } else {
            return joinPoint.proceed();
        }
    }

}
