package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.persistence.EntityManager;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.entitymanager.impl.MultiTenantEntityMgrAspect;

@Aspect
public class CDLMultiTenantEntityMgrAspect extends MultiTenantEntityMgrAspect {

    @Autowired
    private SessionFactory sessionFactory;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    @Qualifier(value = "entityManagerFactory")
    private EntityManager entityManager;

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.CDLJobDetailEntityMgrImpl.find*(..))")
    public void findJobDetailMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.RatingEngineEntityMgrImpl.find*(..))")
    public void findRatingEngineMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.RatingEngineNoteEntityMgrImpl.find*(..))")
    public void findRatingEngineNoteMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.RuleBasedModelEntityMgrImpl.find*(..))")
    public void findRuleBasedModelMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.AIModelEntityMgrImpl.find*(..))")
    public void findAIModelMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.CDLExternalSystemEntityMgrImpl.find*(..))")
    public void allCDLExternalSystemMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.PlayLaunchEntityMgrImpl.find*(..))")
    public void findPlayLaunch(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr, entityManager);
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.PlayLaunchEntityMgrImpl.delete*(..))")
    public void deletePlayLaunch(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr, entityManager);
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.PlayEntityMgrImpl.find*(..))")
    public void findPlay(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr, entityManager);
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.PlayEntityMgrImpl.delete*(..))")
    public void deletePlay(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr, entityManager);
    }
}
