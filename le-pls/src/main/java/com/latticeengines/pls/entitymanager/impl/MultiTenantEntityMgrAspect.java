package com.latticeengines.pls.entitymanager.impl;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.TenantEntityMgr;
import com.latticeengines.pls.security.TicketAuthenticationToken;

@Aspect
public class MultiTenantEntityMgrAspect {

    @Autowired
    private SessionFactory sessionFactory;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Before("execution(* com.latticeengines.pls.entitymanager.impl.ModelSummaryEntityMgrImpl.findAll(..))")
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
        sessionFactory.getCurrentSession().enableFilter("tenantFilter")
                .setParameter("tenantFilterId", tenantWithPid.getPid());
    }

}
