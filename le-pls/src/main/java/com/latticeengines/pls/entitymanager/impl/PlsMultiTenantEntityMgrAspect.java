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

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.ActionEntityMgrImpl.find*(..))")
    public void findAction(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.ActionEntityMgrImpl.update*(..))")
    public void udpateAction(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.ActionEntityMgrImpl.delete*(..))")
    public void deleteAction(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.PdSegmentEntityMgrImpl.find*(..))")
    public void findPdSegment(JoinPoint joinPoint) {
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

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.PdSegmentEntityMgrImpl.update*(..))")
    public void updatePdSegment(JoinPoint joinPoint) {
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

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.PdSegmentEntityMgrImpl.delete*(..))")
    public void deletePdSegment(JoinPoint joinPoint) {
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

    @Before("execution(* com.latticeengines.pls.entitymanager.MarketoCredentialEntityMgr.update*(..))")
    public void updateMarketoCredentialByName(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.MarketoCredentialEntityMgr.find*(..))")
    public void findMarketoCredentialByName(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.MarketoCredentialEntityMgr.delete*(..))")
    public void deleteMarketoCredentialByName(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.EnrichmentEntityMgr.delete*(..))")
    public void deleteEnrichmentById(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.MarketoMatchFieldEntityMgr.update*(..))")
    public void updateMarketoMatchFieldValue(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.PlayEntityMgr.find*(..))")
    public void findPlay(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.PlayEntityMgr.delete*(..))")
    public void deletePlay(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.RatingEngineEntityMgr.find*(..))")
    public void findRatingEngine(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.RatingEngineEntityMgr.delete*(..))")
    public void deleteRatingEngine(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.PlayLaunchEntityMgr.find*(..))")
    public void findPlayLaunch(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.PlayLaunchEntityMgr.delete*(..))")
    public void deletePlayLaunch(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.MetadataSegmentExportEntityMgr.find*(..))")
    public void findMetadataSegmentExport(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.MetadataSegmentExportEntityMgr.delete*(..))")
    public void deleteMetadataSegmentExport(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint, sessionFactory, tenantEntityMgr);
    }

}
