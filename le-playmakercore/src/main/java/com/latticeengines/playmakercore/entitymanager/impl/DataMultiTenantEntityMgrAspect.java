package com.latticeengines.playmakercore.entitymanager.impl;

import javax.inject.Inject;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.hibernate.SessionFactory;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantEntityMgrAspect;

@Aspect
public class DataMultiTenantEntityMgrAspect extends MultiTenantEntityMgrAspect {

    @Inject
    private SessionFactory sessionFactory;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Before("execution(* com.latticeengines.playmakercore.entitymanager.impl.RecommendationEntityMgrImpl.find*(..))")
    public void findRecommendations(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.playmakercore.entitymanager.impl.RecommendationEntityMgrImpl.delete*(..))")
    public void deleteRecommendations(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }

    public void setSessionFactory(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }
}
