package com.latticeengines.security.exposed.util;

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

@Aspect
public class ReadWriteRepositoryAspect extends MultiTenantEntityMgrAspect {

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

    @Before("execution(* com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl.find*(..))")
    public void allFindInBaseReadWriteEntityMgr(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr,
                Arrays.asList(entityManager, entityManagerReader));
    }

}
