package com.latticeengines.apps.cdl.entitymgr.impl;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;

import javax.annotation.Resource;
import javax.inject.Inject;
import javax.persistence.EntityManager;

import org.apache.commons.collections4.CollectionUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.reflect.MethodSignature;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.latticeengines.apps.core.annotation.SoftDeleteConfiguration;
import com.latticeengines.db.exposed.dao.impl.AbstractBaseDaoImpl;
import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.pls.SoftDeletable;
import com.latticeengines.security.exposed.util.MultiTenantEntityMgrAspect;

@Aspect
public class CDLMultiTenantEntityMgrAspect extends MultiTenantEntityMgrAspect {

    private static final Logger log = LoggerFactory.getLogger(CDLMultiTenantEntityMgrAspect.class);

    @Resource(name = "sessionFactory")
    private SessionFactory sessionFactory;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    @Qualifier(value = "entityManagerFactory")
    private EntityManager entityManager;

    @Autowired
    @Qualifier(value = "entityManagerFactoryReader")
    private EntityManager entityManagerReader;

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.*.find*(..))")
    public void find(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr,
                Arrays.asList(entityManager, entityManagerReader));
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.*.find*(..)) || execution(* com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl.find*(..))")
    public void findWithSoftDelete(JoinPoint joinPoint) {
        enableSoftDeleteFilter(joinPoint, sessionFactory, Arrays.asList(entityManager, entityManagerReader));
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.PlayEntityMgrImpl.get*(..))")
    public void getPlay(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr,
                Arrays.asList(entityManager, entityManagerReader));
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.PlayEntityMgrImpl.delete*(..))")
    public void deletePlay(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr,
                Arrays.asList(entityManager, entityManagerReader));
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.PlayLaunchEntityMgrImpl.delete*(..))")
    public void deletePlayLaunch(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr,
                Arrays.asList(entityManager, entityManagerReader));
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.DataFeedEntityMgrImpl.*(..)) && "
            + "!execution(* com.latticeengines.apps.cdl.entitymgr.impl.DataFeedEntityMgrImpl.getAllDataFeeds(..)) && "
            + "!execution(* com.latticeengines.apps.cdl.entitymgr.impl.DataFeedEntityMgrImpl.getAllSimpleDataFeeds(..)) &&"
            + "!execution(* com.latticeengines.apps.cdl.entitymgr.impl.DataFeedEntityMgrImpl.getSimpleDataFeeds(..)) &&"
            + "!execution(* com.latticeengines.apps.cdl.entitymgr.impl.DataFeedEntityMgrImpl.getDataFeeds(..)) &&"
            + "!execution(* com.latticeengines.apps.cdl.entitymgr.impl.DataFeedEntityMgrImpl.getDataFeedsBySchedulingGroup(..))")
    public void allDataFeedMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.DataCollectionEntityMgrImpl.*(..)) &&"
            + " !execution(* com.latticeengines.apps.cdl.entitymgr.impl.DataCollectionEntityMgrImpl.getAllTableName(..))")
    public void allDataCollectionMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.LookupIdMappingEntityMgrImpl.*(..))")
    public void allLookupIdMappingMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr,
                Arrays.asList(entityManager, entityManagerReader));
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.StatisticsContainerEntityMgrImpl.*(..))")
    public void allStatisticsContainerMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.SegmentEntityMgrImpl.*(..))")
    public void allSegmentMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @SuppressWarnings({"rawtypes", "deprecation"})
    private void enableSoftDeleteFilter(JoinPoint joinPoint, SessionFactory sessionFactory,
                                        Collection<EntityManager> entityManagers) {
        Class entityClass = ((AbstractBaseDaoImpl) ((BaseEntityMgr) joinPoint.getTarget()).getDao())
                .getEntityClassReference();
        if (SoftDeletable.class.isAssignableFrom(entityClass)) {
            SoftDeleteConfiguration softDeleteAnnotation = null;
            try {
                softDeleteAnnotation = getSoftDeleteAnnotation(joinPoint);
            } catch (Exception e) {
                log.warn("Error getting the softDeleteAnnotation");
            }
            // annotation does not present means soft delete filter is enabled
            if (softDeleteAnnotation == null) {
                sessionFactory.getCurrentSession().enableFilter("softDeleteFilter");
                if (CollectionUtils.isNotEmpty(entityManagers)) {
                    entityManagers.forEach(entityManager -> //
                            entityManager.unwrap(Session.class).enableFilter("softDeleteFilter"));
                }
            }
        }

    }

    private SoftDeleteConfiguration getSoftDeleteAnnotation(JoinPoint joinPoint)
            throws NoSuchMethodException, SecurityException {
        final String methodName = joinPoint.getSignature().getName();
        final MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        Method method = methodSignature.getMethod();
        SoftDeleteConfiguration annotation = null;
        method = joinPoint.getTarget().getClass().getDeclaredMethod(methodName, method.getParameterTypes());
        annotation = method.getAnnotation(SoftDeleteConfiguration.class);
        return annotation;
    }

}
