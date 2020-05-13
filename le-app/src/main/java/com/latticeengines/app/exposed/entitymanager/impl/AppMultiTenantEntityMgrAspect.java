package com.latticeengines.app.exposed.entitymanager.impl;

import javax.inject.Inject;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.hibernate.SessionFactory;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantEntityMgrAspect;

@Aspect
public class AppMultiTenantEntityMgrAspect extends MultiTenantEntityMgrAspect {

    @Inject
    private SessionFactory sessionFactory;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Before("execution(* com.latticeengines.app.exposed.entitymanager.impl.*.find*(..))")
    public void allEntityMgrMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }
}
