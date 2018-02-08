package com.latticeengines.metadata.infrastructure;

import javax.inject.Inject;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.metadata.entitymgr.impl.TableTypeHolder;
import com.latticeengines.security.exposed.util.MultiTenantEntityMgrAspect;

@Aspect
public class MetadataMultiTenantEntityMgrAspect extends MultiTenantEntityMgrAspect {

    private static final Logger log = LoggerFactory.getLogger(MetadataMultiTenantEntityMgrAspect.class);

    @Inject
    private SessionFactory sessionFactory;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private TableTypeHolder tableTypeHolder;

    @Before("execution(* com.latticeengines.metadata.entitymgr.impl.TableEntityMgrImpl.*(..))")
    public void allTableMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
        log.info("Table type = " + tableTypeHolder.getTableType());
        sessionFactory.getCurrentSession().enableFilter("typeFilter").setParameter("typeFilterId", //
                tableTypeHolder.getTableType().getCode());
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

    @Before("execution(* com.latticeengines.metadata.entitymgr.impl.ColumnRuleResultEntityMgrImpl.*(..))")
    public void allColumnRuleMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.metadata.entitymgr.impl.RowRuleResultEntityMgrImpl.*(..))")
    public void allRowRuleMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.metadata.entitymgr.impl.StatisticsContainerEntityMgrImpl.*(..))")
    public void allStatisticsContainerMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.metadata.entitymgr.impl.DataFeedEntityMgrImpl.*(..)) && " +
            "!execution(* com.latticeengines.metadata.entitymgr.impl.DataFeedEntityMgrImpl.getAllDataFeeds(..)) && " +
            "!execution(* com.latticeengines.metadata.entitymgr.impl.DataFeedEntityMgrImpl.getAllSimpleDataFeeds(..))")
    public void allDataFeedMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

}
