package com.latticeengines.apps.core.aspect;

import java.util.Arrays;

import javax.annotation.Resource;
import javax.inject.Inject;
import javax.persistence.EntityManager;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantEntityMgrAspect;

@Aspect
public class ServiceAppsMultiTenantEntityMgrAspect extends MultiTenantEntityMgrAspect {

    @Resource(name = "sessionFactory")
    private SessionFactory sessionFactory;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    // have to use @Autowire + @Qualifier, other wise cannot init the bean
    @Autowired
    @Qualifier("entityManagerFactory")
    private EntityManager entityManager;

    @Autowired
    @Qualifier("entityManagerFactoryReader")
    private EntityManager entityManagerReader;

    @Before("execution(* com.latticeengines.apps.*.entitymgr.impl.*.find*(..))")
    public void find(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr,
                Arrays.asList(entityManager, entityManagerReader));
    }

    @Before("execution(* com.latticeengines.apps.*.entitymgr.impl.*.delete*(..))")
    public void delete(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr,
                Arrays.asList(entityManager, entityManagerReader));
    }

    @Before("execution(* com.latticeengines.apps.core.entitymgr.impl.ActionEntityMgrImpl.update*(..))")
    public void udpateAction(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr,
                Arrays.asList(entityManager, entityManagerReader));
    }

}
