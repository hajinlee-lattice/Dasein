package com.latticeengines.pls.entitymanager.impl;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.entitymanager.impl.MultiTenantEntityMgrAspect;

@Aspect
public class PlsMultiTenantEntityMgrAspect extends MultiTenantEntityMgrAspect {

    @Autowired
    private SessionFactory sessionFactory;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.ModelSummaryEntityMgrImpl.find*(..))")
    public void findModelSummary(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.SegmentEntityMgrImpl.find*(..))")
    public void findSegment(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.TargetMarketEntityMgrImpl.find*(..))")
    public void findTargetMarket(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.QuotaEntityMgrImpl.find*(..))")
    public void findQuota(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.ProspectDiscoveryOptionEntityMgrImpl.find*(..))")
    public void findProspectDiscoveryOption(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.ModelSummaryEntityMgrImpl.update*(..))")
    public void updateModelSummary(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.SegmentEntityMgrImpl.update*(..))")
    public void updateSegment(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.TargetMarketEntityMgrImpl.update*(..))")
    public void updateTargetMarket(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.QuotaEntityMgrImpl.update*(..))")
    public void updateQuota(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.ProspectDiscoveryOptionEntityMgrImpl.update*(..))")
    public void updateProspectDiscoveryOption(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.ModelSummaryEntityMgrImpl.delete*(..))")
    public void deleteModelSummary(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.SegmentEntityMgrImpl.delete*(..))")
    public void deleteSegment(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.TargetMarketEntityMgrImpl.delete*(..))")
    public void deleteTargetMarket(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.QuotaEntityMgrImpl.delete*(..))")
    public void deleteQuota(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.ProspectDiscoveryOptionEntityMgrImpl.delete*(..))")
    public void deleteProspectDiscoveryOption(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.workflow.entitymanager.impl.ReportEntityMgrImpl.find*(..))")
    public void findReport(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.SourceFileEntityMgrImpl.findBy*(..))")
    public void allSourceFileMethods(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.Oauth2AccessTokenEntityMgrImpl.find*(..))")
    public void findAllAccessToken(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.Oauth2AccessTokenEntityMgrImpl.get*(..))")
    public void getAccessToken(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.Oauth2AccessTokenEntityMgrImpl.createOrUpdate*(..))")
    public void createOrUpdateAccessToken(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }
}
