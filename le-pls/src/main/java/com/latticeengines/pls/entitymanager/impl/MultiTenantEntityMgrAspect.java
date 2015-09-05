package com.latticeengines.pls.entitymanager.impl;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.TicketAuthenticationToken;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

@Aspect
public class MultiTenantEntityMgrAspect {

    @Autowired
    private SessionFactory sessionFactory;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    
    @Before("execution(* com.latticeengines.pls.entitymanager.impl.ModelSummaryEntityMgrImpl.find*(..))")
    public void findModelSummary(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint);
    }
    
    @Before("execution(* com.latticeengines.pls.entitymanager.impl.SegmentEntityMgrImpl.find*(..))")
    public void findSegment(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.ModelSummaryEntityMgrImpl.update*(..))")
    public void updateModelSummary(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.SegmentEntityMgrImpl.update*(..))")
    public void updateSegment(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.ModelSummaryEntityMgrImpl.delete*(..))")
    public void deleteModelSummary(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint);
    }

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.SegmentEntityMgrImpl.delete*(..))")
    public void deleteSegment(JoinPoint joinPoint) {
        enableMultiTenantFilter(joinPoint);
    }

    public void enableMultiTenantFilter(JoinPoint joinPoint) {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (!(auth instanceof TicketAuthenticationToken)) {
            throw new RuntimeException("Problem with multi-tenancy framework.");
        }
        TicketAuthenticationToken token = (TicketAuthenticationToken) auth;
        Tenant tenant = token.getSession().getTenant();
        Tenant tenantWithPid = tenantEntityMgr.findByTenantId(tenant.getId());
        
        if (tenantWithPid == null) {
            throw new RuntimeException("No tenant found with id " + tenant.getId());
        }
        if (tenantWithPid.getPid() == null) {
            throw new RuntimeException("No tenant pid found for tenant with id " + tenant.getId());
        }
        tenant.setPid(tenantWithPid.getPid());
        sessionFactory.getCurrentSession().enableFilter("tenantFilter")
                .setParameter("tenantFilterId", tenantWithPid.getPid());
    }

}
