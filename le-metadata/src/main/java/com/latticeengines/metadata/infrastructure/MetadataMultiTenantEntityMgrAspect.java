package com.latticeengines.metadata.infrastructure;

import java.util.Arrays;

import javax.annotation.Resource;
import javax.inject.Inject;
import javax.persistence.EntityManager;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.metadata.entitymgr.impl.TableTypeHolder;
import com.latticeengines.security.exposed.util.MultiTenantEntityMgrAspect;

@Aspect
public class MetadataMultiTenantEntityMgrAspect extends MultiTenantEntityMgrAspect {

    private static final Logger log = LoggerFactory.getLogger(MetadataMultiTenantEntityMgrAspect.class);

    @Resource(name = "sessionFactory")
    private SessionFactory sessionFactory;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private TableTypeHolder tableTypeHolder;

    @Autowired
    @Qualifier(value = "entityManagerFactoryReader")
    private EntityManager entityManagerReader;

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

    @Before("execution(* com.latticeengines.metadata.entitymgr.impl.ColumnRuleResultEntityMgrImpl.*(..))")
    public void allColumnRuleMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.metadata.entitymgr.impl.RowRuleResultEntityMgrImpl.*(..))")
    public void allRowRuleMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.metadata.entitymgr.impl.AttributeEntityMgrImpl.*(..))")
    public void allAttributeMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr, Arrays.asList(entityManagerReader));
    }

}
