package com.latticeengines.playmakercore.entitymanager.impl;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.entitymanager.impl.MultiTenantEntityMgrAspect;

@Aspect
public class DataMultiTenantEntityMgrAspect extends MultiTenantEntityMgrAspect {

    @Autowired
    private SessionFactory sessionFactory;

    @Autowired
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
