package com.latticeengines.metadata.entitymgr.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.entitymanager.impl.MultiTenantEntityMgrAspect;

@Aspect
public class MetadataMultiTenantEntityMgrAspect extends MultiTenantEntityMgrAspect {
    private static final Log log = LogFactory.getLog(MetadataMultiTenantEntityMgrAspect.class);

    @Autowired
    private SessionFactory sessionFactory;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private TableTypeHolder tableTypeHolder;

    @Before("execution(* com.latticeengines.metadata.entitymgr.impl.TableEntityMgrImpl.*(..))")
    public void allTableMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
        log.info("Table type = " + tableTypeHolder.getTableType());
        sessionFactory.getCurrentSession().enableFilter("typeFilter").setParameter("typeFilterId", //
                tableTypeHolder.getTableType().getCode());

    }

    @Before("execution(* com.latticeengines.metadata.entitymgr.impl.TableTagEntityMgrImpl.*(..))")
    public void allTableTagMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.metadata.entitymgr.impl.ArtifactEntityMgrImpl.*(..))")
    public void allArtifactMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.metadata.entitymgr.impl.ModuleEntityMgrImpl.*(..))")
    public void allModuleMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.metadata.entitymgr.impl.SegmentEntityMgrImpl.*(..))")
    public void allSegmentMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.metadata.entitymgr.impl.DataCollectionEntityMgrImpl.*(..))")
    public void allDataCollectionMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.metadata.entitymgr.impl.DependableObjectEntityMgrImpl.*(..))")
    public void allDependableObjectMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }
}
