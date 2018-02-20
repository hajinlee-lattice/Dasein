package com.latticeengines.db.exposed.aspect;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.db.exposed.util.MultiTenantContext;

@Aspect
public class CleanupMultiTenantContextAspect {

    private static final Logger log = LoggerFactory.getLogger(CleanupMultiTenantContextAspect.class);

    @After("execution(* com.latticeengines.*.controller.*(..))")
    public void afterControllerMethods(JoinPoint joinPoint) {
        log.info("Clean up multi-tenant context");
        MultiTenantContext.setTenant(null);
    }

}
