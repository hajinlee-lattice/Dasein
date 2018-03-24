package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.annotation.Resource;
import javax.inject.Inject;
import javax.persistence.EntityManager;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.latticeengines.db.exposed.dao.impl.AbstractBaseDaoImpl;
import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.pls.SoftDeletable;
import com.latticeengines.security.exposed.util.MultiTenantEntityMgrAspect;

@Aspect
public class CDLMultiTenantEntityMgrAspect extends MultiTenantEntityMgrAspect {

    @Resource(name = "sessionFactory")
    private SessionFactory sessionFactory;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    @Qualifier(value = "entityManagerFactory")
    private EntityManager entityManager;

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.*.find*(..))")
    public void find(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr, entityManager);
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.*.findAll*(..))"
            + "&& !@annotation(com.latticeengines.apps.core.annotation.IncludeDeleted)")
    public void findWithoutDeleted(JoinPoint joinPoint) {
        enableSoftDeleteFilter(joinPoint, sessionFactory, entityManager);
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.PlayLaunchEntityMgrImpl.delete*(..))")
    public void deletePlayLaunch(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr, entityManager);
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.PlayEntityMgrImpl.delete*(..))")
    public void deletePlay(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr, entityManager);
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.DataFeedEntityMgrImpl.*(..)) && "
            + "!execution(* com.latticeengines.apps.cdl.entitymgr.impl.DataFeedEntityMgrImpl.getAllDataFeeds(..)) && "
            + "!execution(* com.latticeengines.apps.cdl.entitymgr.impl.DataFeedEntityMgrImpl.getAllSimpleDataFeeds(..))")
    public void allDataFeedMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.DataCollectionEntityMgrImpl.*(..))")
    public void allDataCollectionMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.StatisticsContainerEntityMgrImpl.*(..))")
    public void allStatisticsContainerMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.SegmentEntityMgrImpl.*(..))")
    public void allSegmentMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @SuppressWarnings({ "rawtypes", "deprecation" })
    private void enableSoftDeleteFilter(JoinPoint joinPoint, SessionFactory sessionFactory,
            EntityManager entityManager) {
        Class entityClass = ((AbstractBaseDaoImpl) ((BaseEntityMgr) joinPoint.getTarget()).getDao())
                .getEntityClassReference();
        if (SoftDeletable.class.isAssignableFrom(entityClass)) {
            sessionFactory.getCurrentSession().enableFilter("softDeleteFilter");
            if (entityManager != null) {
                entityManager.unwrap(Session.class).enableFilter("softDeleteFilter");
            }
        }

    }

}
