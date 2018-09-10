package com.latticeengines.apps.lp.entitymgr.impl;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantEntityMgrAspect;

@Aspect
public class LPMultiTenantEntityMgrAspect extends MultiTenantEntityMgrAspect {

    private static final Logger log = LoggerFactory.getLogger(LPMultiTenantEntityMgrAspect.class);

    @Resource(name = "sessionFactory")
    private SessionFactory sessionFactory;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Before("execution(* com.latticeengines.apps.lp.entitymgr.impl.ModelSummaryEntityMgrImpl.update*(..))")
    public void updateModelSummary(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.apps.lp.entitymgr.impl.ModelSummaryEntityMgrImpl.getAll(..))")
    public void getModelSummaries(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }
}
