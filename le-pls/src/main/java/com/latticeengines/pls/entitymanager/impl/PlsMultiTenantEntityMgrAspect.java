package com.latticeengines.pls.entitymanager.impl;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.entitymanager.impl.MultiTenantEntityMgrAspect;

@Aspect
public class PlsMultiTenantEntityMgrAspect extends MultiTenantEntityMgrAspect {

    @Autowired
    private SessionFactory sessionFactory;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.ModelSummaryEntityMgrImpl.find*(..))")
    public void findModelSummary(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.SegmentEntityMgrImpl.find*(..))")
    public void findSegment(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.ModelSummaryEntityMgrImpl.update*(..))")
    public void updateModelSummary(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.SegmentEntityMgrImpl.update*(..))")
    public void updateSegment(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.ModelSummaryEntityMgrImpl.delete*(..))")
    public void deleteModelSummary(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.SegmentEntityMgrImpl.delete*(..))")
    public void deleteSegment(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

}
