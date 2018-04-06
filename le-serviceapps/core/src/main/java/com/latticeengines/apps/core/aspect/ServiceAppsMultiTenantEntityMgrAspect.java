package com.latticeengines.apps.core.aspect;

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
    @Qualifier(value = "entityManagerFactory")
    private EntityManager entityManager;

    @Before("execution(* com.latticeengines.apps.core.entitymgr.impl.*.find*(..))")
    public void find(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr, entityManager);
    }

    @Before("execution(* com.latticeengines.apps.core.entitymgr.impl.ActionEntityMgrImpl.update*(..))")
    public void udpateAction(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr, entityManager);
    }

    @Before("execution(* com.latticeengines.apps.core.entitymgr.impl.ActionEntityMgrImpl.delete*(..))")
    public void deleteAction(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr, entityManager);
    }

}
